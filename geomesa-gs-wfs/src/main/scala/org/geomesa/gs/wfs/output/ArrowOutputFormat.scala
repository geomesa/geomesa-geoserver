/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import java.io.{BufferedOutputStream, OutputStream}

import com.typesafe.scalalogging.LazyLogging
import org.geomesa.gs.wfs.output.ArrowOutputFormat.Fields
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.ArrowProperties
import org.locationtech.geomesa.arrow.io.FormatVersion
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.process.transform.ArrowConversionProcess.ArrowVisitor
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.sort.SortOrder

import scala.collection.JavaConverters._

/**
  * Output format for wfs requests that encodes features into arrow vector format.
  * To trigger, use outputFormat=application/vnd.arrow in your wfs request
  *
  * Optional flags:
  *   format_options=includeFids:<Boolean>;proxyFids:<Boolean>;dictionaryFields:<field_to_encode>,<field_to_encode>;
  *     useCachedDictionaries:<Boolean>;sortField:<sort_field>;sortReverse:<Boolean>;
  *     batchSize:<Integer>;doublePass:<Boolean>;formatVersion:<String>
  *
  * @param geoServer geoserver
  */
class ArrowOutputFormat(geoServer: GeoServer)
    extends WFSGetFeatureOutputFormat(geoServer, Set("arrow", ArrowOutputFormat.MimeType).asJava) with LazyLogging {

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

    val hints = new Hints()
    hints.put(ARROW_ENCODE, java.lang.Boolean.TRUE)

    val options = request.getFormatOptions.asInstanceOf[java.util.Map[String, String]]
    Option(options.get(Fields.IncludeFids)).foreach { option =>
      hints.put(ARROW_INCLUDE_FID, java.lang.Boolean.valueOf(option))
    }
    Option(options.get(Fields.ProxyFids)).foreach { option =>
      hints.put(ARROW_PROXY_FID, java.lang.Boolean.valueOf(option))
    }
    Option(options.get(Fields.DictionaryFields)).foreach { option =>
      hints.put(ARROW_DICTIONARY_FIELDS, option)
    }
    Option(options.get(Fields.UseCachedDictionaries)).foreach { option =>
      hints.put(ARROW_DICTIONARY_CACHED, java.lang.Boolean.valueOf(option))
    }
    Option(options.get(Fields.FormatVersion)).foreach { option =>
      hints.put(ARROW_FORMAT_VERSION, option)
    }
    Option(options.get(Fields.SortField)).foreach { option =>
      hints.put(ARROW_SORT_FIELD, option)
    }
    Option(options.get(Fields.SortReverse)).foreach { option =>
      hints.put(ARROW_SORT_REVERSE, java.lang.Boolean.valueOf(option))
    }
    Option(options.get(Fields.BatchSize)).foreach { option =>
      hints.put(ARROW_BATCH_SIZE, java.lang.Integer.valueOf(option))
    }
    Option(options.get(Fields.DoublePass)).foreach { option =>
      hints.put(ARROW_DOUBLE_PASS, java.lang.Boolean.valueOf(option))
    }

    // set hints into thread local state - this prevents any wrapping feature collections from messing with
    // the aggregation
    QueryPlanner.setPerThreadQueryHints(hints.asScala.toMap)

    try {
      WithClose(new BufferedOutputStream(output)) { bos =>
        import org.locationtech.geomesa.index.conf.QueryHints.RichHints
        import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

        featureCollections.getFeatures.asScala.foreachIndex { case (fc, i) =>
          WithClose(CloseableIterator(fc.asInstanceOf[SimpleFeatureCollection].features())) { iter =>
            // this check needs to be done *after* getting the feature iterator so that the return sft will be set
            val aggregated = fc.getSchema == org.locationtech.geomesa.arrow.ArrowEncodedSft
            if (aggregated) {
              // with distributed processing, encodings have already been computed in the servers
              iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).foreach(bos.write)
            } else {
              // for non-encoded fs we do the encoding here
              logger.warn(s"Server side arrow aggregation is not enabled for feature collection '${fc.getClass}'")

              val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)
              val dictionaries = hints.getArrowDictionaryFields
              val cacheDictionaries = Some(hints.isArrowCachedDictionaries)
              val version = hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get)
              val sortField = hints.getArrowSort.map(_._1)
              val sortReverse = hints.getArrowSort.map(_._2)
              val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
              val doublePass = hints.isArrowDoublePass

              val preSorted = for (field <- sortField; reverse <- sortReverse.orElse(Some(false))) yield {
                request.getQueries.get(i).getSortBy.asScala match {
                  case Seq(sort) =>
                    Option(sort.getPropertyName).exists(_.getPropertyName == field) &&
                        (sort.getSortOrder == SortOrder.DESCENDING) == reverse
                  case _ => false
                }
              }

              val visitor = new ArrowVisitor(fc.getSchema.asInstanceOf[SimpleFeatureType], encoding, version,
                dictionaries, cacheDictionaries, sortField, sortReverse, preSorted.getOrElse(false), batchSize, doublePass)

              iter.foreach(visitor.visit)

              visitor.getResult().results.asScala.foreach(bos.write)
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
    val FormatVersion         = "FORMATVERSION"
    val SortField             = "SORTFIELD"
    val SortReverse           = "SORTREVERSE"
    val BatchSize             = "BATCHSIZE"
    val DoublePass            = "DOUBLEPASS"
  }
}
