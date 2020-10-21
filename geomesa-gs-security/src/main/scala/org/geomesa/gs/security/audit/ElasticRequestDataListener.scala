/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.security.audit

import java.lang.reflect.Type
import java.net.{MalformedURLException, URL}
import java.util.Date

import com.google.gson._
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.action.DocWriteResponse
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox



class ElasticRequestDataListener extends RequestDataListener with LazyLogging {
  val strUrl = sys.env.getOrElse("ELASTICSEARCH_HOST", null)
  var host : String = "localhost"
  var port = 9200
  var protocol : String = "http"
  try {
    val url = new URL(strUrl)
    host = url.getHost
    port = url.getPort
    if (port == -1){
      port = url.getDefaultPort()
    }
    protocol = url.getProtocol
  }
  catch {
    case e: MalformedURLException =>
      logger.error("Bad URL given. Could not resolve " + strUrl)
      throw new Exception("Given URL cannot be read by Java URL")
  }
  val index = sys.env.getOrElse("GEOSERVER_ES_INDEX", null)
  val client = new RestHighLevelClient(
    RestClient.builder(
      new HttpHost(host, port, protocol)
    )
  )
  private val gson: Gson = new GsonBuilder()
    .registerTypeAdapter(classOf[Date], DateSerializer)
    .registerTypeAdapter(classOf[BoundingBox], BoundingBoxSerializer)
    .registerTypeAdapter(classOf[Throwable], ThrowableSerializer)
    .serializeNulls().create()

  object DateSerializer extends JsonSerializer[Date] {
    override def serialize(src: Date, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(src.getTime)
    }
  }

   object BoundingBoxSerializer extends JsonSerializer[BoundingBox] {
     override def serialize(src: BoundingBox, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
       new JsonPrimitive(src.toString)
     }
   }

  object ThrowableSerializer extends JsonSerializer[Throwable] {
    override def serialize(src: Throwable, typeOfSrc: Type, context: JsonSerializationContext): JsonElement = {
      new JsonPrimitive(src.getMessage)
    }
  }

  override def requestStarted(requestData: RequestData): Unit = {}

  override def requestUpdated(requestData: RequestData): Unit = {
    writeToElasticsearch(requestData)
  }

  override def requestCompleted(requestData: RequestData): Unit = {
  }

  private def writeToElasticsearch(requestData: RequestData) = {
    // 1. Skip over requests which do not have the resources set.
    // 2. Skip over failures without the endTime set.
    if (
      !requestData.getResources.isEmpty &&
      (!(requestData.getStatus == RequestData.Status.FAILED && requestData.getEndTime == null))
    ) {
      val json = gson.toJson(requestData)
      val request = new IndexRequest(index)
      request.id(s"${requestData.getId}:${requestData.getStartTime}")
      request.source(json, XContentType.JSON)
      client.indexAsync(request, RequestOptions.DEFAULT, new LoggingCallback())
    }
  }

  class LoggingCallback() extends ActionListener[IndexResponse] with LazyLogging {
    override def onResponse(index_response :IndexResponse): Unit = {
      val index = index_response.getIndex
      val id = index_response.getId
      if (index_response.getResult eq DocWriteResponse.Result.CREATED) {
        logger.debug("Request indexed in " + index + " with Id " + id)
      }
      val shardInfo = index_response.getShardInfo
      if (shardInfo.getTotal != shardInfo.getSuccessful) {
        logger.debug("Total shards do not match successful shards. Total: " + shardInfo.getTotal + " Successful: " + shardInfo.getSuccessful)
      }
      if (shardInfo.getFailed > 0) for (failure <- shardInfo.getFailures) {
        val reason = failure.reason
        logger.warn("Shard Failure: " + reason)
      }
    }

    override def onFailure(ex: Exception): Unit = {
      logger.error("Index request failed, caused by: " + ex)
    }
  }

  override def requestPostProcessed(requestData: RequestData): Unit = {}
}