/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import org.geoserver.monitor
import org.springframework.util.ReflectionUtils

class RequestData(requestData: monitor.RequestData)
  extends monitor.RequestData with Comparable[RequestData] with Equals {

  def this() = this(new monitor.RequestData)

  ReflectionUtils.shallowCopyFieldState(requestData, this)

  def getStartTimeMillis: Long = Option(getStartTime).map(_.getTime).getOrElse(0L)
  def uid: String = s"$getStartTimeMillis-$internalid"

  override def toString: String = s"RequestData($uid)"
  override def hashCode: Int = internalid.hashCode
  override def compareTo(that: RequestData): Int = this.internalid.compareTo(that.internalid)
  override def canEqual(that: Any): Boolean = that.isInstanceOf[RequestData]
  override def equals(obj: Any): Boolean = {
    obj match {
      case that: RequestData if that.canEqual(this) && (this.compareTo(that) == 0) => true
      case _ => false
    }
  }
}

object RequestData {

  val startTimeOrdering: Ordering[RequestData] = new Ordering[RequestData] {
    override def compare(rd1: RequestData, rd2: RequestData): Int =
      rd1.getStartTimeMillis.compare(rd2.getStartTimeMillis) // sort in ascending order
  }
}
