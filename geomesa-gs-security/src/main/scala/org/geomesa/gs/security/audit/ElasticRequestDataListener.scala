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
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpPost}
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.geoserver.monitor.{RequestData, RequestDataListener}
import org.opengis.geometry.BoundingBox

import scala.concurrent.Await


class ElasticRequestDataListener extends RequestDataListener with LazyLogging {

  private val requestConfig = RequestConfig.custom()
    .setSocketTimeout(10000)
    .setConnectTimeout(10000).build();

  val client: CloseableHttpAsyncClient = HttpAsyncClients.custom()
    .setMaxConnTotal(10)
    .setDefaultRequestConfig(requestConfig)
    .build()

  client.start()
  val host = sys.env.getOrElse("ELASTICSEARCH_HOST", null)
  val index = sys.env.getOrElse("GEOSERVER_ES_INDEX", null)

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
    val post = new HttpPost(host + "/" + index + "/_doc?pretty")
    post.addHeader("Content-Type", "application/json")

    // 1. Skip over requests which do not have the resources set.
    // 2. Skip over failures without the endTime set.
    if (
      !requestData.getResources.isEmpty &&
      (!(requestData.getStatus == RequestData.Status.FAILED && requestData.getEndTime == null))
    ) {
      try{
        val json = gson.toJson(requestData)
        gson.fromJson(json, classOf[Any])
        post.setEntity(new StringEntity(json))
        val future = (client.execute(post, new LoggingCallback(post)))
      } catch {
        case ex: JsonSyntaxException =>
          logger.warn("Invalid JSON format. Proceeding anyways...")
      } finally {

      }
    }
  }

  class LoggingCallback(post : HttpPost) extends FutureCallback[HttpResponse] with LazyLogging {
    override def completed(result: HttpResponse): Unit = {
      logger.info(" - Post Response: " + result.getStatusLine)
      println(" - Post Response: " + result.getStatusLine)
      post.releaseConnection()
    }

    override def failed(ex: Exception): Unit = {
      logger.error("Post Error: " + ex)
      post.releaseConnection()
    }

    override def cancelled(): Unit = {
      logger.warn("Post cancelled")
      post.releaseConnection()
    }
  }
  override def requestPostProcessed(requestData: RequestData): Unit = {}
}