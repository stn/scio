/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify

import java.util.UUID

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.{Dataflow, DataflowScopes}
import com.google.common.reflect.ClassPath
import com.google.datastore.v1._
import com.google.datastore.v1.client.DatastoreHelper
import com.spotify.scio._
import com.spotify.scio.runners.dataflow.DataflowResult
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Aggregator
import org.apache.beam.runners.dataflow.DataflowPipelineJob
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat, PeriodFormat}
import org.joda.time.{DateTimeZone, Instant, Seconds}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

// This file is symlinked to scio-test/src/it/scala/com/spotify/ScioBenchmark.scala so that it can
// run on HEAD. Keep all changes contained in the same file.

object ScioBenchmarkSettings {
  val defaultProjectId: String = "scio-playground"

  val commonArgs = Array(
    "--runner=DataflowRunner",
    "--numWorkers=4",
    "--workerMachineType=n1-standard-4",
    "--autoscalingAlgorithm=NONE")

  val shuffleConf = Map("ShuffleService" -> Array("--experiments=shuffle_mode=service"))
}

// scalastyle:off number.of.methods
object ScioBenchmark {

  import ScioBenchmarkSettings._

  private val dataflow = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential
      .getApplicationDefault.createScoped(Seq(DataflowScopes.CLOUD_PLATFORM).asJava)
    new Dataflow.Builder(transport, jackson, credential).build()
  }

  private val datastore = {
    DatastoreHelper.getDatastoreFromEnv
  }

  private lazy val datastoreMetricKeys = Set("Elapsed", "TotalMemoryUsage", "TotalPdUsage",
    "TotalShuffleDataProcessed", "TotalSsdUsage", "TotalStreamingDataProcessed", "TotalVcpuTime")

  case class CircleCIEnv(buildNum: Long, gitHash: String)

  def getCircleCIEnv(argz: Args): Option[CircleCIEnv] = {
    val isCircleCIRun = sys.env.get("CIRCLECI").contains("true")
    val isTestRun = argz.boolean("testDSIntegration", false)

    if (isCircleCIRun) {
      (sys.env.get("CIRCLE_BUILD_NUM"), sys.env.get("CIRCLE_SHA1")) match {
        case (Some(buildNumber), Some(gitHash)) => Some(CircleCIEnv(buildNumber.toLong, gitHash))
        case _ => throw new IllegalStateException("CIRCLECI env variable is set but not " +
          "CIRCLE_BUILD_NUM and CIRCLE_SHA1")
      }
    } else if (isTestRun) {
      Some(CircleCIEnv(0L, "TEST-ENV-GIT-HASH"))
    } else {
      println("CircleCI env variable not found. Will not publish benchmark results to Datastore")
      None
    }
  }

  case class ScioBenchmarkRun(timestamp: Instant)

  // scalacd style:off
  def main(args: Array[String]): Unit = {
    val argz = Args(args)
    val name = argz("name")
    val regex = argz.getOrElse("regex", ".*")
    val projectId = argz.getOrElse("project", ScioBenchmarkSettings.defaultProjectId)
    val timestamp = DateTimeFormat.forPattern("yyyyMMddHHmmss")
      .withZone(DateTimeZone.UTC)
      .print(System.currentTimeMillis())
    val prefix = s"ScioBenchmark-$name-$timestamp"
    val results = benchmarks
      .filter(_.name.matches(regex))
      .flatMap(_.run(projectId, prefix))
    lazy val circleCIEnv = getCircleCIEnv(argz)
    circleCIEnv.foreach(env => prettyPrint("Circle CI Env: ", env.toString))

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future.sequence(results.map(_.result.finalState))
    Await.result(future, Duration.Inf)

    // scalastyle:off regex
    val metrics = results.map { r =>
      println("=" * 80)
      prettyPrint("Benchmark", r.name)
      prettyPrint("Extra arguments", r.extraArgs.mkString(" "))
      prettyPrint("State", r.result.state.toString)

      val jobId = r.result.internal.asInstanceOf[DataflowPipelineJob].getJobId
      val job = dataflow.projects().jobs().get(projectId, jobId).setView("JOB_VIEW_ALL").execute()
      val parser = ISODateTimeFormat.dateTimeParser()
      prettyPrint("Create time", job.getCreateTime)
      prettyPrint("Finish time", job.getCurrentStateTime)
      val start = parser.parseLocalDateTime(job.getCreateTime)
      val finish = parser.parseLocalDateTime(job.getCurrentStateTime)
      val elapsed = PeriodFormat.getDefault.print(Seconds.secondsBetween(start, finish))
      prettyPrint("Elapsed", elapsed)

      val metrics = r.result.as[DataflowResult].getJobMetrics.getMetrics.asScala
        .filter { m =>
          m.getName.getName.startsWith("Total") && !m.getName.getContext.containsKey("tentative")
        }
        .map(m => (m.getName.getName, m.getScalar.toString))
        .sortBy(_._1)

      metrics.foreach(kv => prettyPrint(kv._1, kv._2))
      OperationBenchmark(r.name,
        metrics.filter(metric => datastoreMetricKeys.contains(metric._1)).toMap)
    }
    if (circleCIEnv.isDefined) { saveMetricsToDataStore(circleCIEnv.get, metrics) }
    // scalastyle:on regex
  }

  case class OperationBenchmark(opName: String, metrics: Map[String, String])

  private def saveMetricsToDataStore(circleCIEnv: CircleCIEnv,
                                     benchmarks: Iterable[OperationBenchmark]): Unit = {
    println("Saving metrics to DataStore...")

    benchmarks.foreach { benchmark =>
      val entity = Entity.newBuilder().setKey(
        DatastoreHelper.makeKey(circleCIEnv.buildNum.toString, benchmark.opName))
      entity.putProperties("gitHash", DatastoreHelper.makeValue(circleCIEnv.gitHash).build())
      entity.putProperties("CIBuildNum", DatastoreHelper.makeValue(circleCIEnv.buildNum).build())
      entity.putProperties("operation", DatastoreHelper.makeValue(benchmark.opName).build())
      entity.putProperties("timestamp", DatastoreHelper.makeValue(Instant.now().toString).build())

      benchmark.metrics.foreach { metric =>
        entity.putProperties(metric._1, DatastoreHelper.makeValue(metric._2).build())
      }

      try {
        datastore.commit(CommitRequest.newBuilder()
          .setMode(CommitRequest.Mode.NON_TRANSACTIONAL)
          .addMutations(Mutation.newBuilder().setUpsert(entity).build())
          .build())
      } catch {
        case e: Exception => println("Caught exception committing to DataStore. Will not " +
          s"publish metrics for operation ${benchmark.opName}")
      }
    }
  }

  private def prettyPrint(k: String, v: String): Unit = {
    // scalastyle:off regex
    println("%-20s: %s".format(k, v))
    // scalastyle:on regex
  }

  // =======================================================================
  // Benchmarks
  // =======================================================================

  private val benchmarks = ClassPath.from(Thread.currentThread().getContextClassLoader)
    .getAllClasses
    .asScala
    .filter(_.getName.matches("com\\.spotify\\.ScioBenchmark\\$[\\w]+\\$"))
    .flatMap { ci =>
      val cls = ci.load()
      if (classOf[Benchmark] isAssignableFrom cls) {
        Some(cls.newInstance().asInstanceOf[Benchmark])
      } else {
        None
      }
    }

  case class BenchmarkResult(name: String, extraArgs: Array[String], result: ScioResult)
  case class BenchmarkMetrics(
                               operation: String,
                               timestamp: Instant,
                               memoryGB: Double,
                               pDiskGB: Double,
                               timeElapsedSeconds: Long
                             )

  abstract class Benchmark(val extraConfs: Map[String, Array[String]] = null) {

    val name: String = this.getClass.getSimpleName.replaceAll("\\$$", "")

    private val configurations: Map[String, Array[String]] = {
      val base = Map(name -> Array.empty[String])
      val extra = if (extraConfs == null) {
        Map.empty
      } else {
        extraConfs.map(kv => (s"$name${kv._1}", kv._2))
      }
      base ++ extra
    }

    def run(projectId: String, prefix: String): Iterable[BenchmarkResult] = {
      val username = sys.props("user.name")
      configurations
        .map { case (confName, extraArgs) =>
          val (sc, _) = ContextAndArgs(Array(s"--project=$projectId") ++ commonArgs ++ extraArgs)
          sc.setAppName(confName)
          sc.setJobName(s"$prefix-$confName-$username".toLowerCase())
          run(sc)
          BenchmarkResult(confName, extraArgs, sc.close())
        }
    }

    def run(sc: ScioContext): Unit
  }

  // ===== Combine =====

  // 100M items, into a set of 1000 unique items

  object Reduce extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).reduce(_ ++ _)
  }

  object Sum extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).sum
  }

  object Fold extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).fold(Set.empty[Int])(_ ++ _)
  }

  object FoldMonoid extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).map(Set(_)).fold
  }

  object Aggregate extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).aggregate(Set.empty[Int])(_ + _, _ ++ _)
  }

  object AggregateAggregator extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000)
        .aggregate(Aggregator.fromMonoid[Set[Int]].composePrepare[Int](Set(_)))
  }

  object Combine extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).map(_.hashCode % 1000).combine(Set(_))(_ + _)(_ ++ _)
  }

  // ===== CombineByKey =====

  // 100M items, 10K keys, into a set of 1000 unique items per key

  object ReduceByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).reduceByKey(_ ++ _)
  }

  object SumByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).sumByKey
  }

  object FoldByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).foldByKey(Set.empty[Int])(_ ++ _)
  }

  object FoldByKeyMonoid extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .mapValues(Set(_)).foldByKey
  }

  object AggregateByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .aggregateByKey(Set.empty[Int])(_ + _, _ ++ _)
  }

  object AggregateByKeyAggregator extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .aggregateByKey(Aggregator.fromMonoid[Set[Int]].composePrepare[Int](Set(_)))
  }

  object CombineByKey extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).keyBy(_ => Random.nextInt(10 * K)).mapValues(_.hashCode % 1000)
        .combineByKey(Set(_))(_ + _)(_ ++ _)
  }

  // ===== GroupByKey =====

  // 100M items, 10K keys, average 10K values per key
  object GroupByKey extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 100 * M).groupBy(_ => Random.nextInt(10 * K)).mapValues(_.size)
  }

  // 10M items, 1 key
  object GroupAll extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomUUIDs(sc, 10 * M).groupBy(_ => 0).mapValues(_.size)
  }

  // ===== Join =====

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 50M items, 5M keys, average 10 values per key
  object Join extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) join randomKVs(sc, 50 * M, 5 * M)
  }

  // LHS: 100M items, 10M keys, average 1 values per key
  // RHS: 50M items, 5M keys, average 1 values per key
  object JoinOne extends Benchmark(shuffleConf) {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 100 * M) join randomKVs(sc, 50 * M, 50 * M)
  }

  // LHS: 100M items, 10M keys, average 10 values per key
  // RHS: 1M items, 100K keys, average 10 values per key
  object HashJoin extends Benchmark {
    override def run(sc: ScioContext): Unit =
      randomKVs(sc, 100 * M, 10 * M) hashJoin randomKVs(sc, M, 100 * K)
  }

  // ===== SideInput =====

  // Main: 100M, side: 1M

  object SingletonSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).map(Set(_)).sum.asSingletonSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).size) }
    }
  }

  object IterableSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asIterableSideInput
      main.withSideInputs(side).map { case (x, s) => (x, s(side).head) }
    }
  }

  object ListSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 100 * M)
      val side = randomUUIDs(sc, 1 * M).asListSideInput
      main.withSideInputs(side)
        .map { case (x, s) => (x, s(side).head) }
    }
  }

  // Main: 1M, side: 100K

  object MapSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 1 * M)
      val side = main
        .sample(withReplacement = false, 0.1)
        .map((_, UUID.randomUUID().toString))
        .asMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  object MultiMapSideInput extends Benchmark {
    override def run(sc: ScioContext): Unit = {
      val main = randomUUIDs(sc, 1 * M)
      val side = main
        .sample(withReplacement = false, 0.1)
        .map((_, UUID.randomUUID().toString))
        .asMultiMapSideInput
      main.withSideInputs(side).map { case (x, s) => s(side).get(x) }
    }
  }

  // =======================================================================
  // Utilities
  // =======================================================================

  private val M = 1000000
  private val K = 1000
  private val numPartitions = 100

  private def randomUUIDs(sc: ScioContext, n: Long): SCollection[String] =
    sc.parallelize(Seq.fill(numPartitions)(n / numPartitions))
      .applyTransform(ParDo.of(new FillDoFn(() => UUID.randomUUID().toString)))

  private def randomKVs(sc: ScioContext,
                        n: Long, numUniqueKeys: Int): SCollection[(String, String)] =
    sc.parallelize(Seq.fill(numPartitions)(n / numPartitions))
      .applyTransform(ParDo.of(new FillDoFn(() =>
        ("key" + Random.nextInt(numUniqueKeys), UUID.randomUUID().toString)
      )))

  private class FillDoFn[T](val f: () => T) extends DoFn[Long, T] {
    @ProcessElement
    def processElement(c: DoFn[Long, T]#ProcessContext): Unit = {
      var i = 0L
      val n = c.element()
      while (i < n) {
        c.output(f())
        i += 1
      }
    }
  }

}
// scalastyle:on number.of.methods
