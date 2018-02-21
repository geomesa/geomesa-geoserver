/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import java.io.{BufferedOutputStream, OutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.process.transform.ArrowConversionProcess.ArrowVisitor
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.sort.SortOrder

import scala.collection.JavaConversions._

/**
  * Output format for wfs requests that encodes features into arrow vector format.
  * To trigger, use outputFormat=application/vnd.arrow in your wfs request
  *
  * Optional flags:
  *   format_options=includeFids:<Boolean>;proxyFids:<Boolean>;dictionaryFields:<field_to_encode>,<field_to_encode>;
  *     useCachedDictionaries:<Boolean>;sortField:<sort_field>;sortReverse:<Boolean>;
  *     batchSize:<Integer>;doublePass:<Boolean>
  *
  * @param geoServer geoserver
  */
class ArrowOutputFormat(geoServer: GeoServer)
    extends WFSGetFeatureOutputFormat(geoServer, Set("arrow", ArrowOutputFormat.MimeType)) with LazyLogging {

  override def getMimeType(value: AnyRef, operation: Operation): String = ArrowOutputFormat.MimeType

  override def getPreferredDisposition(value: AnyRef, operation: Operation): String = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation): String = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    s"$name.${ArrowOutputFormat.FileExtension}"
  }

  override def write(featureCollections: FeatureCollectionResponse,
                     output: OutputStream,
                     getFeature: Operation): Unit = {

    // format_options flags for customizing the request
    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))

    val hints: Map[AnyRef, AnyRef] = {
      import ArrowOutputFormat.Fields

      val builder = Map.newBuilder[AnyRef, AnyRef]
      builder += ARROW_ENCODE -> Boolean.box(true)

      val options = request.getFormatOptions.asInstanceOf[java.util.Map[String, String]]
      Option(options.get(Fields.IncludeFids)).foreach { option =>
        builder += ARROW_INCLUDE_FID -> java.lang.Boolean.valueOf(option)
      }
      Option(options.get(Fields.ProxyFids)).foreach { option =>
        builder += ARROW_PROXY_FID -> java.lang.Boolean.valueOf(option)
      }
      Option(options.get(Fields.DictionaryFields)).foreach { option =>
        builder += ARROW_DICTIONARY_FIELDS -> option
      }
      Option(options.get(Fields.UseCachedDictionaries)).foreach { option =>
        builder += ARROW_DICTIONARY_CACHED -> java.lang.Boolean.valueOf(option)
      }
      Option(options.get(Fields.SortField)).foreach { option =>
        builder += ARROW_SORT_FIELD -> option
      }
      Option(options.get(Fields.SortReverse)).foreach { option =>
        builder += ARROW_SORT_REVERSE -> java.lang.Boolean.valueOf(option)
      }
      Option(options.get(Fields.BatchSize)).foreach { option =>
        builder += ARROW_BATCH_SIZE -> java.lang.Integer.valueOf(option)
      }
      Option(options.get(Fields.DoublePass)).foreach { option =>
        builder += ARROW_DOUBLE_PASS -> java.lang.Boolean.valueOf(option)
      }

      builder.result()
    }

    // set hints into thread local state - this prevents any wrapping feature collections from messing with
    // the aggregation
    QueryPlanner.setPerThreadQueryHints(hints)

    try {
      WithClose(new BufferedOutputStream(output)) { bos =>
        import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

        import scala.collection.JavaConverters._
        featureCollections.getFeatures.asScala.foreachIndex { case (fc, i) =>
          WithClose(CloseableIterator(fc.asInstanceOf[SimpleFeatureCollection].features())) { iter =>
            // this check needs to be done *after* getting the feature iterator so that the return sft will be set
            val aggregated = fc.getSchema == org.locationtech.geomesa.arrow.ArrowEncodedSft
            if (aggregated) {
              // with distributed processing, encodings have already been computed in the servers
              iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).foreach(bos.write(_))
            } else {
              // for non-encoded fs we do the encoding here
              logger.warn(s"Server side arrow aggregation is not enabled for feature collection '${fc.getClass}'")

              val includeFid = hints.get(ARROW_INCLUDE_FID).forall(_.asInstanceOf[Boolean])
              val proxyFid = hints.get(ARROW_PROXY_FID).exists(_.asInstanceOf[Boolean])
              val encoding = SimpleFeatureEncoding.min(includeFid, proxyFid)
              val dictionaries = hints.get(ARROW_DICTIONARY_FIELDS).map(_.asInstanceOf[String].split(",").toSeq).getOrElse(Seq.empty)
              val cacheDictionaries = hints.get(ARROW_DICTIONARY_CACHED).asInstanceOf[Option[Boolean]]
              val sortField = hints.get(ARROW_SORT_FIELD).asInstanceOf[Option[String]]
              val sortReverse = hints.get(ARROW_SORT_REVERSE).asInstanceOf[Option[Boolean]]
              val batchSize = hints.get(ARROW_BATCH_SIZE).asInstanceOf[Option[Int]].getOrElse(ArrowProperties.BatchSize.get.toInt)
              val doublePass = hints.get(ARROW_DOUBLE_PASS).asInstanceOf[Option[Boolean]].getOrElse(false)

              val preSorted = for (field <- sortField; reverse <- sortReverse.orElse(Some(false))) yield {
                request.getQueries.get(i).getSortBy.toSeq match {
                  case Seq(sort) =>
                    Option(sort.getPropertyName).exists(_.getPropertyName == field) &&
                        (sort.getSortOrder == SortOrder.DESCENDING) == reverse
                  case _ => false
                }
              }

              val visitor = new ArrowVisitor(fc.getSchema.asInstanceOf[SimpleFeatureType], encoding, dictionaries,
                cacheDictionaries, sortField, sortReverse, preSorted.getOrElse(false), batchSize, doublePass)

              iter.foreach(visitor.visit)

              visitor.getResult().results.foreach(bos.write(_))
            }
          }
        }
      }
    } finally {
      QueryPlanner.clearPerThreadQueryHints()
    }
  }
}

object ArrowOutputFormat extends LazyLogging {

  val MimeType      = "application/vnd.arrow"
  val FileExtension = "arrow"

  object Fields {
    // note: format option keys are always upper-cased by geoserver
    val IncludeFids           = "INCLUDEFIDS"
    val ProxyFids             = "PROXYFIDS"
    val DictionaryFields      = "DICTIONARYFIELDS"
    val UseCachedDictionaries = "USECACHEDDICTIONARIES"
    val SortField             = "SORTFIELD"
    val SortReverse           = "SORTREVERSE"
    val BatchSize             = "BATCHSIZE"
    val DoublePass            = "DOUBLEPASS"
  }
}
