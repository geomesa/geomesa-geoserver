/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.monitor.elastic

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.elasticsearch.client.{Request, RestClient}
import org.geoserver.monitor.{RequestData, RequestDataListener}

import scala.collection.JavaConverters._

class ElasticRequestDataListener extends RequestDataListener with LazyLogging {

  import org.geomesa.gs.monitor.elastic.ElasticRequestDataListener._

  private val config = ConfigFactory.load().getConfig(GEOSERVER_MONITOR_ELASTICSEARCH_KEY)
  private val index = config.getString("index")
  private val excludedFields =
    if (config.hasPath("excludedFields")) {
      config.getStringList("excludedFields").asScala.toSet
    } else {
      Set.empty[String]
    }

  private val gson = ExtendedRequestData.getGson(excludedFields)
  private val restClient = getRestClient(config)

  override def requestStarted(requestData: RequestData): Unit = {}
  override def requestUpdated(requestData: RequestData): Unit = {}
  override def requestCompleted(requestData: RequestData): Unit = {}
  override def requestPostProcessed(requestData: RequestData): Unit = {
    try {
      writeElasticsearch(restClient, index, gson.toJson(ExtendedRequestData(requestData)))
    } catch {
      case ex: Exception => logger.error(s"Failed to write request to Elasticsearch: ${ex.getMessage}")
    }
  }
}

object ElasticRequestDataListener {

  val GEOSERVER_MONITOR_ELASTICSEARCH_KEY: String = "geomesa.geoserver.monitor.elasticsearch"

  // TODO: Use `co.elastic.clients:elasticsearch-java` client API instead of low-level REST API
  private def writeElasticsearch(client: RestClient, index: String, json: String): Unit = {
    val request = new Request("POST", s"/$index/_doc")
    request.setJsonEntity(json)
    client.performRequest(request)
  }

  private def getRestClient(config: Config): RestClient = {
    val host = config.getString("host")
    val port = config.getInt("port")
    val user = config.getString("user")
    val password = config.getString("password")

    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))

    val clientBuilder = RestClient.builder(new HttpHost(host, port))
      .setHttpClientConfigCallback(new HttpClientConfigCallback {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      })

    clientBuilder.build()
  }
}
