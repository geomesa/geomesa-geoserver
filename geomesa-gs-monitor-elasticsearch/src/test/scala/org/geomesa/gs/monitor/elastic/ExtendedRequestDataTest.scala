/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import org.geomesa.gs.monitor.elastic.ExtendedRequestDataTest._
import org.geoserver.monitor.RequestData
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.jts.io.WKTWriter
import org.specs2.mutable.Specification

import java.nio.charset.StandardCharsets
import java.util.Date
import scala.collection.JavaConverters._

class ExtendedRequestDataTest extends Specification {

  "ExtendedRequestData" should {
    "set its failure status" >> {
      "when there is no error" in {
        val rd = new RequestData
        val erd = ExtendedRequestData(rd)

        Boolean.unbox(erd.failed) must beFalse
      }

      "when there is an error" in {
        val rd = new RequestData
        rd.setError(new UnsupportedOperationException("error"))
        val erd = ExtendedRequestData(rd)

        Boolean.unbox(erd.failed) must beTrue
      }
    }

    "compute the centroid of its bbox" >> {
      "when the bbox is not set" in {
        val rd = new RequestData
        val erd = ExtendedRequestData(rd)

        erd.bboxCentroid must beNull
      }

      "when the bbox is set" in {
        val bbox = new ReferencedEnvelope(-117.14141615693983, -117.19950166515697, 37.034726090346105, 37.09281159856325, CRS)
        val rd = new RequestData
        rd.setBbox(bbox)
        val erd = ExtendedRequestData(rd)

        val expectedWkt = BBOX_CENTROID
        val wkt = WKT_WRITER.write(erd.bboxCentroid)

        wkt mustEqual expectedWkt
      }
    }

    "compute the centroids of the geometries in its query string" >> {
      "when the queryString is not set" in {
        val rd = new RequestData
        val erd = ExtendedRequestData(rd)

        erd.queryCentroids.asScala must beEmpty
      }

      "when there is no cql filter start key" in {
        val queryString = s"INTERSECTS (attr1, $POLYGON_1)"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        erd.queryCentroids.asScala must beEmpty
      }

      "when the cql filter cannot be parsed" in {
        val queryString = s"CQL_FILTER=INTERSECTS (attr2, $POLYGON_2);service=WFS;srsName=EPSG:4326"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        erd.queryCentroids.asScala must beEmpty
      }

      "when there are no attributes in the query" in {
        val queryString = "CQL_FILTER=INCLUDE"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        erd.queryCentroids.asScala must beEmpty
      }

      "when there are no geometries in the query" in {
        val queryString = "CQL_FILTER=attr1 LIKE 'foo'"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        erd.queryCentroids.asScala must beEmpty
      }

      "when there query contains a polygon" in {
        val queryString = s"CQL_FILTER=INTERSECTS (attr1, $POLYGON_1)"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_1).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the cql filter start key is lower case" in {
        val queryString = s"cql_filter=INTERSECTS (attr1, $POLYGON_1)"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_1).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains metadata" in {
        val queryString = s"CQL_FILTER=INTERSECTS (attr1, $POLYGON_1)&service=WFS&srsName=EPSG:4326"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_1).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains a bbox" in {
        val queryString = "CQL_FILTER=BBOX (attr1, -117.14141615693983, 37.034726090346105, -117.19950166515697, 37.09281159856325)"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(BBOX_CENTROID).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains two attributes" in {
        val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_1)) AND (INTERSECTS (attr2, $POLYGON_2))"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_1, CENTROID_2).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains two AND'd polygons, where one is encapsulated" in {
        val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_1)) AND (INTERSECTS (attr1, $POLYGON_2))"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_2).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains two AND'd polygons, where they are intersecting" in {
        val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_3)) AND (INTERSECTS (attr1, $POLYGON_4))"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_3_AND_4).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains two OR'd polygons, where they are nonconvergent" in {
        val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_3)) OR (INTERSECTS (attr1, $POLYGON_5))"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_3, CENTROID_5).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }

      "when the query contains a polygon AND'd with a bbox" in {
        val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_6)) AND (BBOX (attr1, -97.04690330744837, 32.904622353605006, -97.04664673865905, 32.90487892239433))&"
        val rd = new RequestData
        rd.setQueryString(queryString)
        val erd = ExtendedRequestData(rd)

        val expectedWkts = List(CENTROID_6).asJava
        val wkts = erd.queryCentroids.asScala.map(WKT_WRITER.write).asJava

        wkts mustEqual expectedWkts
      }
    }

    "serialize to JSON" >> {
      val bbox = new ReferencedEnvelope(-117.14141615693983, -117.19950166515697, 37.034726090346105, 37.09281159856325, CRS)
      val body = new String("body").getBytes(StandardCharsets.UTF_8)
      val bodyContentLength = new java.lang.Long(4)
      val category = RequestData.Category.REST
      val endTime = new Date(1645048281995L)
      val error = new UnsupportedOperationException("error")
      val host = "127.0.0.1"
      val httpMethod = "GET"
      val queryString = s"CQL_FILTER=(INTERSECTS (attr1, $POLYGON_3)) OR (INTERSECTS (attr1, $POLYGON_5))"
      val resources = List("foo:bar").asJava
      val responseStatus = new java.lang.Integer(200)
      val service = "WFS"
      val startTime = new Date(1645048277230L)
      val status = RequestData.Status.FINISHED
      val totalTime = new java.lang.Long(4765)

      val rd = new RequestData
      rd.setBbox(bbox)
      rd.setBody(body)
      rd.setBodyContentLength(bodyContentLength)
      rd.setCategory(category)
      rd.setEndTime(endTime)
      rd.setError(error)
      rd.setHost(host)
      rd.setHttpMethod(httpMethod)
      rd.setQueryString(queryString)
      rd.setResources(resources)
      rd.setResponseStatus(responseStatus)
      rd.setService(service)
      rd.setStartTime(startTime)
      rd.setStatus(status)
      rd.setTotalTime(totalTime)

      val erd = ExtendedRequestData(rd)

      // null fields should be excluded regardless
      val excludedFields = Set("httpMethod", "responseLength", "responseContentType", "subOperation", "remoteLat", "remoteLon", "id", "internalid")

      val gson = ExtendedRequestData.getGson(excludedFields)
      val json = gson.toJson(erd)
      val expectedJson = s"""{"failed":true,"bbox_centroid":"$BBOX_CENTROID","query_centroids":["$CENTROID_3","$CENTROID_5"],"status":"$status","category":"$category","query_string":"$queryString","body":"${new String(body, StandardCharsets.UTF_8)}","body_content_length":$bodyContentLength,"start_time":${startTime.getTime},"end_time":${endTime.getTime},"total_time":$totalTime,"host":"$host","service":"$service","resources":${resources.asScala.mkString("[\"","\",\"","\"]")},"error":"${error.getMessage}","response_status":$responseStatus,"bbox":"${bbox.toString}"}"""

      json mustEqual expectedJson
    }
  }
}

object ExtendedRequestDataTest {

  private val WKT_WRITER = new WKTWriter()
  private val CRS = DefaultGeographicCRS.WGS84

  private val POLYGON_1 = "POLYGON ((120 35, 120 40, 115 40, 115 35, 120 35))"
  private val POLYGON_2 = "POLYGON ((117 37, 117 38, 116 38, 116 37, 117 37))"
  private val POLYGON_3 = "POLYGON ((1 1, 4 1, 4 3, 1 3, 1 1))"
  private val POLYGON_4 = "POLYGON ((2 2, 3 2, 3 4, 2 4, 2 2))"
  private val POLYGON_5 = "POLYGON ((2 4, 3 4, 3 5, 2 5, 2 4))"
  private val POLYGON_6 = "POLYGON ((-98.3411610007752 33.84983160588202, -98.3411610007752 32.92210622610406, -98.3411610007752 31.994380846326116, -96.78433429050405 31.994380846326116, -95.2275075802329 31.994380846326116, -95.2275075802329 32.92210622610406, -95.2275075802329 33.84983160588202, -96.78433429050405 33.84983160588202, -98.3411610007752 33.84983160588202))"

  private val CENTROID_1 = "POINT (117.5 37.5)"
  private val CENTROID_2 = "POINT (116.5 37.5)"
  private val CENTROID_3 = "POINT (2.5 2)"
  private val CENTROID_3_AND_4 = "POINT (2.5 2.5)"
  private val CENTROID_5 = "POINT (2.5 4.5)"
  private val CENTROID_6 = "POINT (-97.04677502305373 32.90475063799967)"

  private val BBOX_CENTROID = "POINT (-117.1704589110484 37.06376884445468)"
}