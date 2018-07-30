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

package com.spotify.scio.coders

import scala.collection.JavaConverters._
import org.apache.beam.sdk.util.CoderUtils
import com.twitter.bijection._, Bijection._
import org.apache.avro.generic.GenericRecord
import org.scalatest.{FlatSpec, Matchers, Assertion}
import com.spotify.scio.values.SCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder.gen
import com.spotify.scio.coders.Implicits._

final case class UserId(bytes: Seq[Byte])

final case class User(id: UserId, username: String, email: String)

sealed trait Top
final case class TA(anInt: Int, aString: String) extends Top
final case class TB(anDouble: Double) extends Top

case class DummyCC(s: String)

case class ParameterizedDummy[A](value: A)

case class MultiParameterizedDummy[A, B](valuea: A, valueb: B)

class CodersTest extends FlatSpec with Matchers {

  val userId = UserId(Array[Byte](1, 2, 3, 4))
  val user = User(userId, "johndoe", "johndoe@spotify.com")

  private def checkSer[A](implicit c: Coder[A]) = {
    val beamCoder = Coder.beamWithDefault(c)
    org.apache.beam.sdk.util.SerializableUtils.ensureSerializable(beamCoder)
  }

  import org.scalactic.Equality
  def check[T](t: T)(implicit C: Coder[T], eq: Equality[T]): Assertion = {
    val beamCoder = Coder.beamWithDefault(C)
    org.apache.beam.sdk.util.SerializableUtils.ensureSerializable(beamCoder)
    val enc = CoderUtils.encodeToByteArray(beamCoder, t)
    val dec = CoderUtils.decodeFromByteArray(beamCoder, enc)
    dec should === (t)
  }

  "Coders" should "support primitives" in {
    check(1)
    check("yolo")
    check(4.5)
  }

  it should "support Scala collections" in {
    val nil: Seq[String] = Nil
    val s: Seq[String] = (1 to 10).toSeq.map(_.toString)
    val m = s.map{ v => v.toString -> v }.toMap
    check(nil)
    check(s)
    check(s.toList)
    check(m)
    check(s.toSet)
  }

  it should "support Java collections" in {
    import java.util.{ List => jList, Map => jMap }
    val is = (1 to 10).toSeq
    val s: jList[String] = is.map(_.toString).asJava
    val m: jMap[String, Int] = is.map{ v => v.toString -> v }.toMap.asJava
    check(s)
    check(m)
  }

  it should "support Java POJOs ?" ignore {
    ???
  }

  object Avro {
    import com.spotify.scio.avro.{ User => AvUser, Account, Address }
    val accounts: List[Account] = List(new Account(1, "tyoe", "name", 12.5))
    val address = new Address("street1", "street2", "city", "state", "01234", "Sweden")
    val user = new AvUser(1, "lastname", "firstname", "email@foobar.com", accounts.asJava, address)

    val eq = new Equality[GenericRecord]{
      def areEqual(a: GenericRecord, b: Any): Boolean =
        a.toString === b.toString // YOLO
    }
  }

  it should "Derive serializable coders" in {
    checkSer[Int]
    checkSer[String]
    checkSer[List[Int]]
    checkSer(Coder.fallback[Int])
    checkSer(gen[(Int, Int)])
    checkSer(gen[DummyCC])
    checkSer[com.spotify.scio.avro.User]
  }

  it should "support Avro's SpecificRecordBase" in {
    check(Avro.user)
  }

  it should "support Avro's GenericRecord" in {
    val schema = Avro.user.getSchema
    val record: GenericRecord = Avro.user
    check(record)(genericRecordCoder(schema), Avro.eq)
  }

  it should "derive coders for product types" in {
    check(DummyCC("dummy"))
    check(DummyCC(""))
    check(ParameterizedDummy("dummy"))
    check(MultiParameterizedDummy("dummy", 2))
    check(user)
    check((1, "String", List[Int]()))
    val ds = (1 to 10).map{ _ => DummyCC("dummy") }.toList
    check(ds)
  }

  it should "derive coders for sealed class hierarchies" in {
    val ta: Top = TA(1, "test")
    val tb: Top = TB(4.2)
    check(ta)
    check(tb)
    check((123, "hello", ta, tb, List(("bar", 1, "foo"))))

  }

  it should "support all the already supported types" ignore {

    // see: AlgebirdRegistrar

    // InstantCoder
    // TableRowJsonCoder
    // SpecificRecordBase
    // Message
    // LocalDate
    // LocalTime
    // LocalDateTime
    // DateTime
    // Path
    // ByteString
    // BigDecimal
    // KV
    ???
  }

  private def withSCollection[T: Coder](fn: SCollection[T] => Assertion): Assertion = {
    val sc = ScioContext.forTest()
    val coll = sc.parallelize(Nil: List[T])
    val res = fn(coll)
    sc.close().waitUntilFinish()
    res
  }

  // it should "provide a fallback if no safe coder is available" in
  //   withSCollection[Unit] {
  //     scoll =>
  //       val coders = scoll.context.coders
  //       import org.apache.avro.generic.GenericRecord
  //       val schema = avro.user.getSchema
  //       val record: GenericRecord = avro.user
  //       illTyped("""check(record)""")

  //       {
  //         import coders.fallback
  //         check(record)
  //       }
  //   }

  // it should "not use a fallback if a safe coder is available" in
  //   withSCollection[Unit] { scoll =>
  //     val coders =  scoll.context.coders
  //     import coders.fallback
  //     illTyped("SCoder[DummyCC]") // ambiguous implicit values
  //     succeed
  //   }
}
