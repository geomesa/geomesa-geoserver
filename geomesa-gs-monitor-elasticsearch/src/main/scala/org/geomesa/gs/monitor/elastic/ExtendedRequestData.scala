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
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.StringUtils
import org.geoserver.catalog.{Catalog, ResourceInfo, StoreInfo}
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Point
import org.opengis.filter.Filter
import org.opengis.geometry.BoundingBox

import java.lang.reflect.Type
import java.util.Date
import javax.naming.ldap.LdapName
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ExtendedRequestData(requestData: RequestData, catalog: Catalog) extends RequestData(requestData) {

  import org.geomesa.gs.monitor.elastic.ExtendedRequestData._

  val failed: java.lang.Boolean =
    Option(requestData.getError)
      .isDefined

  val timedOut: java.lang.Boolean =
    Option(requestData.getError)
      .map(_.getMessage)
      .contains(TIMEOUT_KEY)

  val bboxCentroid: Point =
    Option(requestData.getBbox)
      .map(JTS.toGeometry)
      .map(_.getCentroid)
      .orNull

  @transient
  private val queryFilter: Option[Filter] =
    Option(requestData.getQueryString)
      .flatMap(extractCqlFilter)

  val queryAttributes: java.util.List[String] =
    queryFilter
      .map(FilterHelper.propertyNames)
      .asNonEmptyJavaListOrNull

  val queryCentroids: java.util.List[Point] =
    queryFilter
      .map(extractFilterCentroids)
      .asNonEmptyJavaListOrNull

  @transient
  private val distinguishedName: Option[LdapName] =
    Option(requestData.getRemoteUser)
      .flatMap(extractDistinguishedName)

  val commonNames: java.util.List[String] =
    distinguishedName
      .map(extractRdnValues(_, "CN"))
      .asNonEmptyJavaListOrNull

  val organizations: java.util.List[String] =
    distinguishedName
      .map(extractRdnValues(_, "O"))
      .asNonEmptyJavaListOrNull

  val organizationalUnits: java.util.List[String] =
    distinguishedName
      .map(extractRdnValues(_, "OU"))
      .asNonEmptyJavaListOrNull

  val resourceNames: java.util.List[String] = handleExtractResource(requestData, catalog, extractResourceName)
  val resourceStoreNames: java.util.List[String] = handleExtractResource(requestData, catalog, extractStoreName)
  val resourceStoreTitles: java.util.List[String] = handleExtractResource(requestData, catalog, extractResourceTitle)
  val resourceStoreWorkspaces: java.util.List[String] = handleExtractResource(requestData, catalog, extractStoreWorkspace)
  val resourceStoreTypes: java.util.List[String] = handleExtractResource(requestData, catalog, extractResourceStoreType)
}

object ExtendedRequestData extends LazyLogging {

  val CQL_FILTER_START_KEY = "CQL_FILTER="
  val CQL_FILTER_END_KEY = "&"

  val TIMEOUT_KEY = "TIMEOUT"

  def getGson(excludedFields: Set[String]): Gson = {
    new GsonBuilder()
      .registerTypeAdapter(classOf[Point], new JsonSerializer[Point] {
        override def serialize(point: Point, `type`: Type, context: JsonSerializationContext): JsonElement = {
          new JsonPrimitive(WKTUtils.write(point))
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
          new JsonPrimitive(Base64.encodeBase64String(array))
        }
      })
      .addSerializationExclusionStrategy(new ExclusionStrategy {
        override def shouldSkipClass(`class`: Class[_]): Boolean = false
        override def shouldSkipField(fieldAttributes: FieldAttributes): Boolean = {
          excludedFields.contains(fieldAttributes.getName)
        }
      })
      .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
      .disableHtmlEscaping
      .create
  }

  private def extractFilterCentroids(filter: Filter): Seq[Point] = {
    FilterHelper.propertyNames(filter).flatMap {
      FilterHelper.extractGeometries(filter, _).values.map(_.getCentroid)
    }
  }

  private def extractCqlFilter(queryString: String): Option[Filter] = {
    val filterStartKeyIdx = StringUtils.indexOfIgnoreCase(queryString, CQL_FILTER_START_KEY)
    if (filterStartKeyIdx < 0) return None

    val filterStartIdx = filterStartKeyIdx + StringUtils.length(CQL_FILTER_START_KEY)
    val filterStart = queryString.substring(filterStartIdx)
    val filterEndKeyIdx = StringUtils.indexOfIgnoreCase(filterStart, CQL_FILTER_END_KEY)
    val filterString = if (filterEndKeyIdx < 0) filterStart else filterStart.substring(0, filterEndKeyIdx)

    Try(ECQL.toFilter(filterString)).orLog { ex =>
      s"Failed to parse filter from '$queryString': ${ex.getMessage}"
    }
  }

  private def extractDistinguishedName(dnString: String): Option[LdapName] = {
    Try(new LdapName(dnString)).orLog { ex =>
      s"Failed to parse distinguished name from '$dnString': ${ex.getMessage}"
    }
  }

  private def extractRdnValues(dn: LdapName, key: String): Seq[String] = {
    dn.getRdns.asScala.filter(_.getType.equalsIgnoreCase(key)).map(_.getValue.toString).toSeq
  }

  private def extractResourceInfo(resource: String, catalog: Catalog): ResourceInfo = {
    catalog.getLayerByName(resource).getResource
  }

  private def extractResourceName(resource: String, catalog: Catalog): String = {
    extractResourceInfo(resource, catalog)
      .getName
  }

  private def extractResourceStore(resource: String, catalog: Catalog): StoreInfo = {
    extractResourceInfo(resource, catalog)
      .getStore
  }

  private def extractResourceTitle(resource: String, catalog: Catalog): String = {
    extractResourceInfo(resource, catalog)
      .getTitle
  }

  private def extractStoreName(resource: String, catalog: Catalog): String = {
    extractResourceStore(resource, catalog)
      .getName
  }

  private def extractStoreWorkspace(resource: String, catalog: Catalog): String = {
    extractResourceStore(resource, catalog)
      .getWorkspace
      .getName
  }

  private def extractResourceStoreType(resource: String, catalog: Catalog): String = {
    extractResourceStore(resource, catalog)
      .getType
  }

  private def handleExtractResource(requestData: RequestData,
                                    catalog: Catalog,
                                    extractor: (String, Catalog) => String): java.util.List[String] = {
    requestData.getResources
      .asScala
      .map(extractor(_, catalog))
      .asJava
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

  private implicit class OptionSeqExtensions[T](optSeq: Option[Seq[T]]) {
    def asNonEmptyJavaListOrNull: java.util.List[T] = optSeq.filter(_.nonEmpty).map(_.asJava).orNull
  }
}
