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
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox

import scala.collection.JavaConverters.mapAsScalaMapConverter

// TODO externalize hostname:port in a constructor and sample applicationContext.xml
class ElasticRequestDataListener(val defaultHost : String)  extends RequestDataListener{
  val client: CloseableHttpClient = HttpClients.createDefault
  val hostVar = sys.env.getOrElse("ELASTICSEARCH_HOST", defaultHost)
  var post = new HttpPost(hostVar + "/geoserver/_doc?pretty")
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
      println(s"Request Data: \n$json")
      try {
        post.setEntity(new StringEntity(json))
        val response = client.execute(post)
        println("RESPONSE: " + response)
        post.releaseConnection()
      }
      // TODO: Switch to Async request.
    }
  }

  override def requestPostProcessed(requestData: RequestData): Unit = {}
}