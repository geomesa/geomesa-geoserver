/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import org.geoserver.monitor
import org.springframework.util.ReflectionUtils

class RequestData(requestData: monitor.RequestData) extends monitor.RequestData {

  def this() = this(new monitor.RequestData)

  ReflectionUtils.shallowCopyFieldState(requestData, this)

  def getStartTimeMillis: Long = Option(getStartTime).map(_.getTime).getOrElse(0L)
  def uid: String = s"$getStartTimeMillis-$internalid"
}
