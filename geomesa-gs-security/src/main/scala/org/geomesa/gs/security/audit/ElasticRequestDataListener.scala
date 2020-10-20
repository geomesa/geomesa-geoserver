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
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionListener
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.action.DocWriteResponse
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox

class ElasticRequestDataListener extends RequestDataListener {
  val host = sys.env.get("ELASTICSEARCH_HOST")
  val index = sys.env.get("GEOSERVER_ES_INDEX")
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
      val request = new IndexRequest("geoserver")
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