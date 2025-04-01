/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.features.avro.io.AvroDataFileWriter
import org.locationtech.geomesa.utils.io.WithClose

import java.io.OutputStream
import java.util.Collections

class AvroOutputFormat(gs: GeoServer)
    extends WFSGetFeatureOutputFormat(gs, Collections.singleton("application/vnd.avro")) {

  override def getMimeType(value: scala.Any, operation: Operation): String = "application/vnd.avro"

  override def getCapabilitiesElementName: String = "AVRO"

  override def getPreferredDisposition(value: scala.Any, operation: Operation): String = Response.DISPOSITION_ATTACH

  override def getAttachmentFileName(value: scala.Any, operation: Operation): String = {
    val req = GetFeatureRequest.adapt(operation.getParameters.apply(0))
    val outputFileName = req.getQueries.get(0).getTypeNames.get(0).getLocalPart
    s"$outputFileName.avro"
  }

  override def write(fcr: FeatureCollectionResponse,
                     os: OutputStream,
                     operation: Operation): Unit = {
    val features = fcr.getFeatures.get(0).asInstanceOf[SimpleFeatureCollection]
    WithClose(new AvroDataFileWriter(os, features.getSchema))(_.append(features))
  }
}
