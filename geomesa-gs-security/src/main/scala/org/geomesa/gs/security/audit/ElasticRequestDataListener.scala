/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.security.audit

import java.lang.reflect.Type
import java.util.Date

import com.google.gson._
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox
import org.apache.logging.log4j.LogManager
import org.apache.http.util.EntityUtils


class ElasticRequestDataListener  extends RequestDataListener{
  val logger = LogManager.getLogger(classOf[ElasticRequestDataListener])

  val client: CloseableHttpClient = HttpClients.createDefault
  val host = sys.env.getOrElse("ELASTICSEARCH_HOST", null)
  val index = sys.env.getOrElse("GEOSERVER_ES_INDEX", null)
  var post = new HttpPost(host + "/" + index + "/_doc?pretty")
  post.addHeader("Content-Type", "application/json")

  private val gson: Gson = new GsonBuilder()
    .registerTypeAdapter(classOf[Date], new DateSerializer)
    .registerTypeAdapter(classOf[BoundingBox], new BoundingBoxSerializer)
    .registerTypeAdapter(classOf[Throwable], new ThrowableSerializer)
    .serializeNulls().create()

  class DateSerializer extends JsonSerializer[Date] {
    override def serialize(src: Date, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(src.getTime)
    }
  }

   class BoundingBoxSerializer extends JsonSerializer[BoundingBox] {
     override def serialize(src: BoundingBox, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
       new JsonPrimitive(src.toString)
     }
   }

  class ThrowableSerializer extends JsonSerializer[Throwable] {
    override def serialize(src: Throwable, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(src.getMessage)
    }
  }

  override def requestStarted(requestData: RequestData): Unit = {}

  override def requestUpdated(requestData: RequestData): Unit = {
    writeToElasticsearch(requestData)
  }

  override def requestCompleted(requestData: RequestData): Unit = {
    //writeToElasticsearch(requestData)
  }

  private def writeToElasticsearch(requestData: RequestData) = {
    // 1. Skip over requests which do not have the resources set.
    // 2. Skip over failures without the endTime set.
    if (
      !requestData.getResources.isEmpty &&
      (!(requestData.getStatus == RequestData.Status.FAILED && requestData.getEndTime == null))
    ) {
      val json = gson.toJson(requestData)
      try {
        gson.fromJson(json, classOf[Any])
      } catch {
        case ex: JsonSyntaxException =>
          logger.warn("Invalid JSON format. Proceeding anyways...")
      }
      post.setEntity(new StringEntity(json))
      val response : CloseableHttpResponse = client.execute(post)
      try{
        val entity = response.getEntity
        val content = EntityUtils.toString(entity)
        logger.info("Post Response: " + content)
        println(content)
      } finally {
        response.close()
      }
      // TODO: Switch to Async request.
    }
  }

  override def requestPostProcessed(requestData: RequestData): Unit = {}
}