/*
 * Copyright 2016 Spotify AB.
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

import org.apache.beam.sdk.{coders => bcoders}
import org.apache.beam.sdk.coders.{ Coder => BCoder, _}

//
// Java Coders
//
trait JavaCoders {

  implicit def uriCoder: Coder[java.net.URI] =
    Coder.xmap(Coder.beam(StringUtf8Coder.of()))(s => new java.net.URI(s), _.toString)

  implicit def pathCoder: Coder[java.nio.file.Path] =
    Coder.xmap(Coder.beam(StringUtf8Coder.of()))(s => java.nio.file.Paths.get(s), _.toString)

  import java.lang.{Iterable => jIterable}
  implicit def jIterableCoder[T](implicit c: Coder[T]): Coder[jIterable[T]] =
    Coder.transform(c) { bc =>
      Coder.beam(bcoders.IterableCoder.of(bc))
    }

  implicit def jlistCoder[T](implicit c: Coder[T]): Coder[java.util.List[T]] =
    Coder.transform(c) { bc =>
      Coder.beam(bcoders.ListCoder.of(bc))
    }

  implicit def jMapCoder[K, V](implicit ck: Coder[K], cv: Coder[V]): Coder[java.util.Map[K, V]] =
    Coder.transform(ck) { bk =>
      Coder.transform(cv) { bv =>
        Coder.beam(bcoders.MapCoder.of(bk, bv))
      }
    }

  private def fromScalaCoder[J <: java.lang.Number, S <: AnyVal](coder: Coder[S]): Coder[J] =
    coder.asInstanceOf[Coder[J]]

  implicit val jIntegerCoder: Coder[java.lang.Integer] = fromScalaCoder(Coder.intCoder)
  implicit val jLongCoder: Coder[java.lang.Long] = fromScalaCoder(Coder.longCoder)
  implicit val jDoubleCoder: Coder[java.lang.Double] = fromScalaCoder(Coder.doubleCoder)
  // TODO: Byte, Float, Short

  // implicit def mutationCaseCoder: Coder[com.google.bigtable.v2.Mutation.MutationCase] = ???
  // implicit def mutationCoder: Coder[com.google.bigtable.v2.Mutation] = ???

  import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, IntervalWindow}
  // implicit def boundedWindowCoder: Coder[BoundedWindow] =
  //   Coder.beam(BoundedWindow.getCoder())
  implicit def intervalWindowCoder: Coder[IntervalWindow] =
    Coder.beam(IntervalWindow.getCoder())

  // implicit def paneinfoCoder: Coder[PaneInfo] = ???
  implicit def instantCoder: Coder[org.joda.time.Instant] = Coder.beam(InstantCoder.of())
  implicit def tablerowCoder: Coder[com.google.api.services.bigquery.model.TableRow] =
    Coder.beam(org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder.of())
  implicit def messageCoder: Coder[org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage] =
    Coder.beam(org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder.of())
  // implicit def entityCoder: Coder[com.google.datastore.v1.Entity] = ???
}