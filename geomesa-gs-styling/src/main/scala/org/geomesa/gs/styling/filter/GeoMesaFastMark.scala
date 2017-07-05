/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.styling.filter

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class GeoMesaFastMark extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaFastMark",
  parameter("geomesaFastMark", classOf[String]),
  parameter("icon", classOf[String]),
  parameter("rotation", classOf[Double]))) {

  override def evaluate(o: java.lang.Object): AnyRef = {
    val icon = getExpression(0).evaluate(o).asInstanceOf[java.lang.String]
    val rotation: java.lang.Double = Option(getExpression(1).evaluate(o).asInstanceOf[java.lang.Double]).getOrElse(0.0d)
    (icon, rotation)
  }
}