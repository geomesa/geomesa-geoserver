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
    parameter("numberFormat", classOf[String]),
    parameter("property1", classOf[String]),
    parameter("property2", classOf[String]),
    parameter("property3", classOf[String]))) {

  val reg = ".+\\w.+"

  def formatProp (prop: String, format: String): String = {
    if (prop == "") {
      ""
    } else {
      if (prop.matches(reg)) { prop } else {
        try {
          jDouble.parseDouble(prop).formatted(format)
        } catch {
          case _: NumberFormatException => prop
        }
      }
    }
  }

  override def evaluate(o: jObject): AnyRef = {
    val exp1 = getExpression(1).evaluate(o)
    val exp2 = getExpression(2).evaluate(o)
    val exp3 = getExpression(3).evaluate(o)

    val property1 = if (exp1 == null) { "" } else exp1.toString
    val property2 = if (exp2 == null) { "" } else exp2.toString
    val property3 = if (exp3 == null) { "" } else exp3.toString

    if (property1 + property2 + property3 == "") {
      ""
    } else {
      val nf: String = getExpression(0).evaluate(null).asInstanceOf[String]
      formatProp(property1, nf) + "\n" + formatProp(property2, nf) + "\n" + formatProp(property3, nf)
    }
  }
}
