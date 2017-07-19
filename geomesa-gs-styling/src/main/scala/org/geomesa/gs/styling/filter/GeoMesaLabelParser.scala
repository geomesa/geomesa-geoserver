/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.styling.filter

import java.lang.{Double => jDouble, Object => jObject}

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class GeoMesaLabelParser extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaParseLabel",
    parameter("geomesaParseLabel", classOf[String]),
    parameter("property", classOf[String]),
    parameter("numberFormat", classOf[String]))) {

  override def evaluate(o: jObject): AnyRef = {
    val property = getExpression(0).evaluate(o).asInstanceOf[String]
    val numberFormat: String = Option(getExpression(1).evaluate(o).asInstanceOf[String]).getOrElse("%.4f")
    try {
      val prop = jDouble.parseDouble(property)
      if (numberFormat.matches("%.\\d+f")) {
        prop.formatted(numberFormat).toString
      } else {
        prop.toString
      }
    } catch {
      case _: NumberFormatException => property
    }
  }
}
