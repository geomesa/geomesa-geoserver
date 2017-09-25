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
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileWriter
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

/**
  * Output format for wfs requests that encodes features into arrow vector format.
  * To trigger, use outputFormat=application/vnd.arrow in your wfs request
  *
  * Optional flags:
  *   format_options=encode:<field_to_encode>,<field_to_encode>
  *
  * @param geoServer geoserver
  */
class ArrowOutputFormat(geoServer: GeoServer)
    extends WFSGetFeatureOutputFormat(geoServer, Set("arrow", ArrowOutputFormat.MimeType)) with LazyLogging {

  import ArrowOutputFormat.{EncodeField, FileExtension}
  import org.locationtech.geomesa.arrow.allocator

  override def getMimeType(value: AnyRef, operation: Operation): String = ArrowOutputFormat.MimeType

  override def getPreferredDisposition(value: AnyRef, operation: Operation): String = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation): String = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    s"$name.$FileExtension"
  }

  override def write(featureCollections: FeatureCollectionResponse,
                     output: OutputStream,
                     getFeature: Operation): Unit = {

    // format_options flags for customizing the request
    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val encode  = Option(request.getFormatOptions.get(EncodeField).asInstanceOf[String]).getOrElse("")

    val bos = new BufferedOutputStream(output)

    // set hints into thread local state - this prevents any wrapping feature collections from messing with
    // the aggregation
    val hints = Map[AnyRef, AnyRef](ARROW_ENCODE -> Boolean.box(true), ARROW_DICTIONARY_FIELDS -> encode)
    QueryPlanner.setPerThreadQueryHints(hints)

    try {
      featureCollections.getFeatures.foreach { fc =>
        val iter = CloseableIterator(fc.asInstanceOf[SimpleFeatureCollection].features())
        try {
          // this check needs to be done *after* getting the feature iterator so that the return sft will be set
          val aggregated = fc.getSchema == org.locationtech.geomesa.arrow.ArrowEncodedSft
          if (aggregated) {
            // for accumulo, encodings have already been computed in the tservers
            iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).foreach(bos.write)
          } else {
            logger.warn(s"Server side arrow aggregation is not enabled for feature collection '${fc.getClass}'")
            // for non-accumulo fs we do the encoding here
            WithClose(new SimpleFeatureArrowFileWriter(fc.getSchema.asInstanceOf[SimpleFeatureType], bos, Map.empty)) { writer =>
              var i = 0
              iter.foreach { sf =>
                writer.add(sf)
                i += 1
                if (i % 10000 == 0) {
                  writer.flush()
                }
              }
            }
          }
        } finally {
          iter.close()
        }
      }
    } finally {
      QueryPlanner.clearPerThreadQueryHints()
    }
    // none of the implementations in geoserver call 'close' on the output stream
    // our writer will close the underlying stream though...
  }
}

object ArrowOutputFormat extends LazyLogging {

  val MimeType      = "application/vnd.arrow"
  val FileExtension = "arrow"
  val EncodeField   = "encode"
}
