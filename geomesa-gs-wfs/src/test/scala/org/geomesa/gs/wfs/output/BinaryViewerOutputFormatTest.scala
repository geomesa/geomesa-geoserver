/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BinaryViewerOutputFormatTest extends Specification {

  "BinaryViewerOutputFormat" should {

    val format = new BinaryViewerOutputFormat(null)

    "return the correct mime type" in {
      val mimeType = format.getMimeType(null, null)
      mimeType mustEqual "application/vnd.binary-viewer"
    }
  }
}