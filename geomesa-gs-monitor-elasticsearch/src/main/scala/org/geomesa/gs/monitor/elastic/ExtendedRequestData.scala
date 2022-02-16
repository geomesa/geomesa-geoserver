/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import com.google.gson._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.lang3.StringUtils
import org.geomesa.gs.monitor.elastic.ExtendedRequestData.{extractCqlFilter, extractFilterGeometries}
import org.geoserver.monitor.RequestData
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.jts.io.WKTWriter
import org.opengis.filter.Filter
import org.opengis.geometry.BoundingBox
import org.springframework.util.ReflectionUtils

import java.lang.reflect.Type
import java.nio.charset.StandardCharsets
import java.util.Date
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ExtendedRequestData(requestData: RequestData) extends RequestData {

  ReflectionUtils.shallowCopyFieldState(requestData, this)
  
  val failed: java.lang.Boolean =
    Option(requestData.getError)
      .isDefined

  val bboxCentroid: Point =
    Option(requestData.getBbox)
      .map(JTS.toGeometry)
      .map(_.getCentroid)
      .orNull

  val queryCentroids: java.util.List[Point] =
    Option(requestData.getQueryString)
      .flatMap(extractCqlFilter)
      .map(extractFilterGeometries)
      .getOrElse(Seq.empty[Geometry])
      .map(_.getCentroid)
      .asJava
}

object ExtendedRequestData extends LazyLogging {

  val CQL_FILTER_START_KEY = "CQL_FILTER="
  val CQL_FILTER_END_KEY = "&"

  def apply(requestData: RequestData): ExtendedRequestData = new ExtendedRequestData(requestData)

  def getGson(excludedFields: Set[String]): Gson = {
    new GsonBuilder()
      .registerTypeAdapter(classOf[Point], new JsonSerializer[Point] {
        private val WKT_WRITER = new WKTWriter()
        override def serialize(point: Point, `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(WKT_WRITER.write(point))
        }
      })
      .registerTypeAdapter(classOf[Date], new JsonSerializer[Date] {
        override def serialize(date: Date, `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(date.getTime)
        }
      })
      .registerTypeAdapter(classOf[BoundingBox], new JsonSerializer[BoundingBox] {
        override def serialize(bbox: BoundingBox, `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(bbox.toString)
        }
      })
      .registerTypeAdapter(classOf[Throwable], new JsonSerializer[Throwable] {
        override def serialize(ex: Throwable, `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(ex.getMessage)
        }
      })
      .registerTypeAdapter(classOf[Array[Byte]], new JsonSerializer[Array[Byte]] {
        override def serialize(array: Array[Byte], `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(Option(array).map(bytes => new String(bytes, StandardCharsets.UTF_8)).getOrElse(new String))
        }
      })
      .addSerializationExclusionStrategy(new ExclusionStrategy {
        override def shouldSkipClass(`class`: Class[_]): Boolean = false
        override def shouldSkipField(fieldAttributes: FieldAttributes): Boolean = {
          excludedFields.contains(fieldAttributes.getName)
        }
      })
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .disableHtmlEscaping()
      .create()
  }

  def extractFilterGeometries(filter: Filter): Seq[Geometry] = {
    FilterHelper.propertyNames(filter).flatMap { attribute =>
      FilterHelper.extractGeometries(filter, attribute).values
    }
  }

  def extractCqlFilter(queryString: String): Option[Filter] = {
    val filterStartKeyIdx = StringUtils.indexOfIgnoreCase(queryString, CQL_FILTER_START_KEY)
    if (filterStartKeyIdx < 0) return None

    val filterStartIdx = filterStartKeyIdx + StringUtils.length(CQL_FILTER_START_KEY)
    val filterStart = queryString.substring(filterStartIdx)
    val filterEndKeyIdx = StringUtils.indexOfIgnoreCase(filterStart, CQL_FILTER_END_KEY)
    val filterEndIdx = if (filterEndKeyIdx < 0) filterStart.length else filterEndKeyIdx
    val filterString = filterStart.substring(0, filterEndIdx)

    Try(ECQL.toFilter(filterString)).orLog { ex =>
      s"Failed to parse filter from query string: ${ex.getMessage}"
    }
  }

  private implicit class TryExtensions[T](result: Try[T]) {
    def orLog(getMessage: => Throwable => String = ex => ex.getMessage): Option[T] = {
      result match {
        case Success(value) =>
          Option(value)
        case Failure(ex) =>
          logger.error(getMessage(ex))
          None
      }
    }
  }
}