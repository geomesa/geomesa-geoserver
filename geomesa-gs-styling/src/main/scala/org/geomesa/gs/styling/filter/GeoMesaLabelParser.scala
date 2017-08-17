/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.styling.filter

import java.lang.{Double => jDouble, Object => jObject}
import java.util.regex.Pattern

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class GeoMesaLabelParser1 extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaParseLabel_1",
    parameter("geomesaParseLabel_1", classOf[String]),
    parameter("numberFormat", classOf[String]),
    parameter("property1", classOf[String]))) {

  import GeoMesaLabelParser._

  override def evaluate(o: jObject): AnyRef = {
    val expr1 = getExpression(1).evaluate(o)
    val nf: String = getExpression(0).evaluate(null).asInstanceOf[String]
    formatProp(expr1, nf)
  }
}

class GeoMesaLabelParser2 extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaParseLabel_2",
    parameter("geomesaParseLabel_2", classOf[String]),
    parameter("numberFormat", classOf[String]),
    parameter("property1", classOf[String]),
    parameter("property2", classOf[String]))) {

  import GeoMesaLabelParser._

  override def evaluate(o: jObject): AnyRef = {
    val expr1 = getExpression(1).evaluate(o)
    val expr2 = getExpression(2).evaluate(o)
    val nf: String = getExpression(0).evaluate(null).asInstanceOf[String]
    formatProp(expr1, nf) + "\n" + formatProp(expr2, nf)
  }
}

class GeoMesaLabelParser3 extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaParseLabel_3",
    parameter("geomesaParseLabel_3", classOf[String]),
    parameter("numberFormat", classOf[String]),
    parameter("property1", classOf[String]),
    parameter("property2", classOf[String]),
    parameter("property3", classOf[String]))) {

  import GeoMesaLabelParser._

  override def evaluate(o: jObject): AnyRef = {
    val expr1 = getExpression(1).evaluate(o)
    val expr2 = getExpression(2).evaluate(o)
    val expr3 = getExpression(3).evaluate(o)
    val nf: String = getExpression(0).evaluate(null).asInstanceOf[String]
    formatProp(expr1, nf) + "\n" + formatProp(expr2, nf) + "\n" + formatProp(expr3, nf)
  }
}

class GeoMesaLabelParser4 extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaParseLabel_4",
    parameter("geomesaParseLabel_4", classOf[String]),
    parameter("numberFormat", classOf[String]),
    parameter("property1", classOf[String]),
    parameter("property2", classOf[String]),
    parameter("property3", classOf[String]),
    parameter("property4", classOf[String]))) {

  import GeoMesaLabelParser._

  override def evaluate(o: jObject): AnyRef = {
    val expr1 = getExpression(1).evaluate(o)
    val expr2 = getExpression(2).evaluate(o)
    val expr3 = getExpression(3).evaluate(o)
    val expr4 = getExpression(4).evaluate(o)
    val nf: String = getExpression(0).evaluate(null).asInstanceOf[String]
    formatProp(expr1, nf) + "\n" + formatProp(expr2, nf) + "\n" + formatProp(expr3, nf) + "\n" + formatProp(expr4, nf)
  }
}

object GeoMesaLabelParser {
  val p = Pattern.compile("\\d*\\.\\d*")
  def formatProp (expr: Any, format: String): String = {
    if (expr == null) {
      ""
    } else {
      // expr could be an Int/Float/Double
      val prop = expr.toString
      // Only attempt parse if there is a decimal point
      if (!p.matcher(prop).matches || prop == "") {
        prop
      } else {
        try {
          jDouble.parseDouble(prop).formatted(format)
        } catch {
          case _: NumberFormatException => prop
        }
      }
    }
  }
}
