/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import org.geoserver.wfs.request.GetFeatureRequest
import org.geoserver.wfs.{GetFeatureCallback, GetFeatureContext, WFSGetFeatureOutputFormat}
import org.geotools.util.factory.Hints

trait FormatOptionsCallback extends GetFeatureCallback {

  this: WFSGetFeatureOutputFormat =>

  override def beforeQuerying(context: GetFeatureContext): Unit = {
    if (getOutputFormats.contains(context.getRequest.getOutputFormat)) {
      populateFormatOptions(context.getRequest, context.getQuery.getHints)
    }
  }

  /**
   * Extracts the format_options from the request and populates query hints
   *
   * @param request request
   * @param hints   hints to populate
   */
  protected def populateFormatOptions(request: GetFeatureRequest, hints: Hints): Unit
}
