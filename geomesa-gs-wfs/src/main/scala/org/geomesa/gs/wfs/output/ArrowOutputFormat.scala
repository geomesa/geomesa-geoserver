/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import com.typesafe.scalalogging.LazyLogging
import org.geomesa.gs.wfs.output.ArrowOutputFormat.Fields
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.sort.SortOrder
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io.{FormatVersion, SimpleFeatureArrowFileReader}
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.arrow.{ArrowEncodedSft, ArrowProperties}
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.process.transform.ArrowConversionProcess.ArrowVisitor
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{BufferedOutputStream, ByteArrayInputStream, OutputStream, SequenceInputStream}
import scala.collection.JavaConverters._

/**
  * Output format for wfs requests that encodes features into arrow vector format.
  * To trigger, use outputFormat=application/vnd.arrow in your wfs request
  *
  * Optional flags:
  *   format_options=includeFids:<Boolean>;proxyFids:<Boolean>;dictionaryFields:<field_to_encode>,<field_to_encode>;
  *     useCachedDictionaries:<Boolean>;sortField:<sort_field>;sortReverse:<Boolean>;
  *     batchSize:<Integer>;doublePass:<Boolean>;formatVersion:<String>;
  *     flattenStruct:<Boolean>;flipAxisOrder:<Boolean>;
  *
  * @param geoServer geoserver
  */
class ArrowOutputFormat(geoServer: GeoServer)
    extends WFSGetFeatureOutputFormat(geoServer, Set("arrow", ArrowOutputFormat.MimeType).asJava)
        with FormatOptionsCallback with LazyLogging {

  override def getMimeType(value: AnyRef, operation: Operation): String = ArrowOutputFormat.MimeType

  override def getCapabilitiesElementName: String = "ARROW"

  override def getPreferredDisposition(value: AnyRef, operation: Operation): String = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation): String = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    s"$name.${ArrowOutputFormat.FileExtension}"
  }

  override def write(
      featureCollections: FeatureCollectionResponse,
      output: OutputStream,
      getFeature: Operation): Unit = {

    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val hints = new Hints()
    populateFormatOptions(request, hints)

    WithClose(new BufferedOutputStream(output)) { bos =>
      var i = -1
      featureCollections.getFeatures.asScala.foreach { fc =>
        i += 1
        val sfc = fc.asInstanceOf[SimpleFeatureCollection]
        WithClose(CloseableIterator(sfc.features())) { iter =>
          val featureType = sfc.getSchema
          val aggregated = SimpleFeatureTypes.compare(featureType, ArrowEncodedSft) match {
            case 0 | 1 => true
            case _ => false
          }
          if (!aggregated) {
            // for non-encoded fs we do the encoding here
            logger.warn(s"Server side arrow aggregation not enabled for feature collection: ${fc.getClass}, SFT type: ${featureType.getTypeName}")
            arrowVisitor(request, hints, i, bos, iter, featureType)
          } else if (hints.isArrowFlatten) {
            if (!hints.isArrowProcessDeltas) {
              // not able to read the byte arrays with a SimpleFeatureArrowFileReader because
              // they're a custom format that includes threading keys and dictionary deltas
              throw new NotImplementedError(s"${getClass.getTypeName}: incompatible request parameters: 'flattenStruct' and 'processDeltas'")
            }
            WithClose(SimpleFeatureArrowFileReader.caching(featureSequenceInputStream(iter))) { reader =>
              arrowVisitor(request, hints, i, bos, reader.features(), reader.sft)
            }
          } else {
            // with distributed processing, encodings have already been computed in the servers
            iter.map(toByteArray).foreach(bos.write)
          }
        }
      }
    }
  }

  override protected def populateFormatOptions(request: GetFeatureRequest, hints: Hints): Unit = {
    val options = request.getFormatOptions
    hints.put(ARROW_ENCODE, java.lang.Boolean.TRUE)
    Option(options.get(Fields.IncludeFids)).foreach { option =>
      hints.put(ARROW_INCLUDE_FID, java.lang.Boolean.valueOf(option.toString))
    }
    Option(options.get(Fields.ProxyFids)).foreach { option =>
      hints.put(ARROW_PROXY_FID, java.lang.Boolean.valueOf(option.toString))
    }
    Option(options.get(Fields.DictionaryFields)).foreach { option =>
      hints.put(ARROW_DICTIONARY_FIELDS, option.toString)
    }
    Option(options.get(Fields.FormatVersion)).foreach { option =>
      hints.put(ARROW_FORMAT_VERSION, option.toString)
    }
    Option(options.get(Fields.SortField)).foreach { option =>
      hints.put(ARROW_SORT_FIELD, option.toString)
    }
    Option(options.get(Fields.SortReverse)).foreach { option =>
      hints.put(ARROW_SORT_REVERSE, java.lang.Boolean.valueOf(option.toString))
    }
    Option(options.get(Fields.BatchSize)).foreach { option =>
      hints.put(ARROW_BATCH_SIZE, java.lang.Integer.valueOf(option.toString))
    }
    Option(options.get(Fields.ProcessDeltas)).foreach { option =>
      hints.put(ARROW_PROCESS_DELTAS, java.lang.Boolean.valueOf(option.toString))
    }
    Option(options.get(Fields.FlattenStruct)).foreach { option =>
      hints.put(ARROW_FLATTEN_STRUCT, java.lang.Boolean.valueOf(option.toString))
    }
    Option(options.get(Fields.FlipAxisOrder)).foreach { option =>
      hints.put(FLIP_AXIS_ORDER, java.lang.Boolean.valueOf(option.toString))
    }
  }

  private def isPreSorted(request: GetFeatureRequest, index: Int, sortField: Option[String], sortReverse: Option[Boolean]): Boolean =
    (for (field <- sortField; reverse <- sortReverse.orElse(Some(false))) yield {
      request.getQueries.get(index).getSortBy match {
        case list if list.size == 1 =>
          val sort = list.get(0)
          Option(sort.getPropertyName).exists(_.getPropertyName == field) &&
            (sort.getSortOrder == SortOrder.DESCENDING) == reverse
        case _ => false
      }
    }).getOrElse(false)

  private def arrowVisitor(request: GetFeatureRequest, hints: Hints, index: Int,
                           os: OutputStream, features: CloseableIterator[SimpleFeature], sft: SimpleFeatureType): Unit = {
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid, hints.isFlipAxisOrder)
    val version = hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get)
    val dictionaries = hints.getArrowDictionaryFields
    val sortField = hints.getArrowSort.map(_._1)
    val sortReverse = hints.getArrowSort.map(_._2)
    val preSorted = isPreSorted(request, index, sortField, sortReverse)
    val batchSize = hints.getArrowBatchSize.getOrElse(ArrowProperties.BatchSize.get.toInt)
    val flattenStruct = hints.isArrowFlatten

    val visitor = new ArrowVisitor(sft, encoding, version, dictionaries, sortField, sortReverse, preSorted, batchSize, flattenStruct)
    features.foreach(visitor.visit)
    visitor.getResult().results.asScala.foreach(os.write)
  }

  private def toByteArray(feature: SimpleFeature): Array[Byte] =
    feature.getAttribute(0).asInstanceOf[Array[Byte]]

  private def featureSequenceInputStream(iter: CloseableIterator[SimpleFeature]): SequenceInputStream = {
    val streamEnumeration = iter.map(toByteArray).map(new ByteArrayInputStream(_)).asJavaEnumeration
    new SequenceInputStream(streamEnumeration)
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
    val FormatVersion         = "FORMATVERSION"
    val SortField             = "SORTFIELD"
    val SortReverse           = "SORTREVERSE"
    val BatchSize             = "BATCHSIZE"
    val ProcessDeltas         = "PROCESSDELTAS"
    val FlattenStruct         = "FLATTENSTRUCT"
    val FlipAxisOrder         = "FLIPAXISORDER"
  }
}
