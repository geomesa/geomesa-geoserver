/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

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

import scala.collection.mutable.ArrayBuffer



class ElasticRequestDataListener extends RequestDataListener with LazyLogging {
  import ElasticRequestDataListener.gson
  val envVars = sys.env.getOrElse("ELASTICSEARCH_HOST", null).split(',')
  //initialized variables
  var host = ""
  var port = 0
  var protocol = ""
  val hostList = new ArrayBuffer[HttpHost]

  for (u <- envVars) {
    try {
      val url = new URL(u.trim())
      host = url.getHost
      port = url.getPort
      protocol = url.getProtocol
      hostList.append(new HttpHost(host, port, protocol))
    }
    catch{
      case e: MalformedURLException => logger.error("Invalid URL " + u + ". Could not convert from string to URL. Trying any additional URLs")
    }
  }
  if (hostList.length == 0) {
    logger.error("No URL given. Could not resolve " + envVars)
    throw new Exception("Given URL(s) cannot be read by Java URL")
  }

  val hosts = hostList.toArray
  val client = new RestHighLevelClient(
    RestClient.builder(hosts:_*)
  )
  logger.debug("Sending requests to " + hosts)

  var index = sys.env.getOrElse("GEOSERVER_ES_INDEX", null)
  if (index == null){
    index = "geoserver"
    logger.warn("No index name provided. Index will be set to default name 'geoserver'")
  }

  override def requestStarted(requestData: RequestData): Unit = {}

  override def requestUpdated(requestData: RequestData): Unit = {
    writeToElasticsearch(requestData)
  }

  override def requestCompleted(requestData: RequestData): Unit = {}

  private def writeToElasticsearch(requestData: RequestData) = {
    // 1. Skip over requests which do not have the resources set.
    // 2. Skip over failures without the endTime set.
    if (
      !requestData.getResources.isEmpty &&
      (!(requestData.getStatus == RequestData.Status.FAILED && requestData.getEndTime == null))
    ) {
      val json = gson.toJson(requestData)
      val request = new IndexRequest(index)
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

object ElasticRequestDataListener {
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

}