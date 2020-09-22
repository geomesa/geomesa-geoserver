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
import org.apache.http.HttpHost
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox

// TODO externalize hostname:port in a constructor and sample applicationContext.xml
class ElasticRequestDataListener extends RequestDataListener {
  val client = new RestHighLevelClient(
    RestClient.builder(
      new HttpHost("localhost", 9200, "http")
    )
  )
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

  override def requestUpdated(requestData: RequestData): Unit = {}

  override def requestCompleted(requestData: RequestData): Unit = {

    val json = gson.toJson(requestData)
    println(s"Request Data: \n$json")
    import org.elasticsearch.action.index.IndexRequest
    import org.elasticsearch.common.xcontent.XContentType
    val request = new IndexRequest("geoserver")
    request.source(json, XContentType.JSON)
    // TODO: Switch to Async request.
    client.index(request, RequestOptions.DEFAULT)
  }

  override def requestPostProcessed(requestData: RequestData): Unit = {}
}