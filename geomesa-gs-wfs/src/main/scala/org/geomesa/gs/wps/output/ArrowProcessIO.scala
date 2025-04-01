/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wps.output

import org.geomesa.gs.wfs.output.ArrowOutputFormat
import org.geoserver.wps.ppio.BinaryPPIO
import org.geoserver.wps.ppio.ProcessParameterIO.PPIODirection

import java.io.{InputStream, OutputStream}
import scala.collection.JavaConverters._

/**
  * WPS output formatting for arrow. Does not support input formatting.
  */
class ArrowProcessIO extends BinaryPPIO(classOf[java.util.Iterator[Array[Byte]]],
  classOf[java.util.Iterator[Array[Byte]]], ArrowOutputFormat.MimeType) {

  override def getDirection: PPIODirection = PPIODirection.ENCODING

  override def encode(value: AnyRef, os: OutputStream): Unit =
    value.asInstanceOf[java.util.Iterator[Array[Byte]]].asScala.foreach(os.write)

  override def decode(input: InputStream): AnyRef = throw new NotImplementedError("Only supports encode")

  override def getFileExtension: String = ".arrow"
}
