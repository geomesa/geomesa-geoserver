/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.styling

import java.awt.{Graphics2D, Shape}
import java.lang.{Float => jFloat, String => jString}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.LiteralExpressionImpl
import org.geotools.geometry.jts.TransformedShape
import org.geotools.renderer.style._
import org.opengis.feature.Feature
import org.opengis.filter.expression.Expression

class GeoMesaMarkFactory extends TTFMarkFactory {
  // To use this factory utilize the 'geomesaFastMark' ogc function

   val cache = Caffeine.newBuilder().build[String, Array[Shape]](
      new CacheLoader[String, Array[Shape]] {
        override def load(key: String): Array[Shape] = {
          val notRotated = GeoMesaMarkFactory.lookupShape(key)
          if (notRotated != null) {
            Array.tabulate[Shape](360) { i =>
              val ts = new TransformedShape()
              ts.shape = notRotated
              ts.rotate(-i * Math.PI / 180)
              ts
            }
          } else {
            Array.tabulate[Shape](360) { null }
          }
        }
      }
   )

  override def getShape(graphics: Graphics2D, symbolUrl: Expression, feature: Feature): Shape = {
    symbolUrl.evaluate(feature, classOf[(jString, jFloat)]) match {
      case (icon: jString, rotation: jFloat) =>
        val normRotation: Int = {
          if (rotation < 0) {
            (rotation + 360).toInt
          } else {
            rotation.toInt
          }
        }
        require(normRotation >= 0 && normRotation <= 360)
        cache.get(icon)(normRotation)
      case _ => null
    }
  }
}

object GeoMesaMarkFactory extends LazyLogging {
  val ttfFactory = new TTFMarkFactory

  def lookupShape(url: String): Shape = {
    if (url.startsWith("ttf://")) {
      logger.debug(s"Looking up shape for url $url")
      val exp = new LiteralExpressionImpl(url)
      val shape = ttfFactory.getShape(null, exp, null)
      shape
    } else {
      null
    }
  }
}
