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
import org.geomesa.gs.monitor.elastic.ExtendedRequestData.TIMEOUT_KEY
import org.geoserver.monitor
import org.geoserver.monitor.RequestData.Status
import org.geoserver.monitor.RequestDataListener

import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

class ElasticRequestDataListener extends RequestDataListener with LazyLogging {

  import org.geomesa.gs.monitor.elastic.ElasticRequestDataListener._

  private val config = ConfigFactory.load.getConfig(GEOSERVER_MONITOR_ELASTICSEARCH_KEY)
  private val index = config.getString("index")

  private val excludedFields =
    Option("excludedFields")
      .filter(config.hasPath)
      .map(config.getStringList)
      .map(_.asScala.toSet)
      .getOrElse(Set.empty)
  private val timeout =
    Option("timeout")
      .filter(config.hasPath)
      .map(config.getLong)
      .filter(ms => ms > 1000L && ms < 1000000L)
      .getOrElse(10000L)

  private val gson = ExtendedRequestData.getGson(excludedFields)
  private val restClient = getRestClient(config)

  /*
   * GeoServer Request Monitor Timeout Strategy
   *
   * If a request times out, it never reaches the completed or post-processed callbacks and thus
   * would never be sent to Elasticsearch. To combat this, all in-progress requests are stored
   * in a set ordered by their start time. Whenever a request is post-processed, it is sent to
   * Elasticsearch and removed from the set. Any in-progress requests that exceed the configured
   * timeout are marked as failed and sent to Elasticsearch. If a request completes after it has
   * already been marked as a timeout, the Elasticsearch index is updated using the request id.
   */

  private val writeQueue = new LinkedBlockingQueue[RequestData]
  private val requestsInProgress = new SortedHashSet(RequestData.startTimeOrdering)

  private val elasticsearchWriter = new Runnable {
    override def run(): Unit = {
      while (!Thread.interrupted) {
        val rd = writeQueue.take // block until data is available to send

        try {
          putElasticsearch(new ExtendedRequestData(rd))
          logger.debug(s"Sent request '${rd.uid}' to Elasticsearch")
        } catch {
          case ex: Exception => logger.error(s"Failed to send request '${rd.uid}' to Elasticsearch: ${ex.getMessage}")
        }
      }
    }
  }

  private val timeoutHandler = new Runnable {
    override def run(): Unit = {
      // don't read in-progress requests while they are being updated to maintain ordering
      requestsInProgress.synchronized {
        val currentMillis = System.currentTimeMillis
        val timedOutRequests = requestsInProgress.takeWhile(_.getStartTimeMillis + timeout <= currentMillis)

        // mark timed-out requests as failures
        timedOutRequests.foreach { rd =>
          logger.debug(s"Request '${rd.uid}' timed out")
          val ex = new TimeoutException(TIMEOUT_KEY)
          rd.setStatus(Status.FAILED)
          rd.setError(ex)
          rd.setErrorMessage(ex.getMessage)
        }

        // submit requests to be sent to Elasticsearch
        writeQueue.addAll(timedOutRequests.asJavaCollection)
      }
    }
  }

  private val executor = Executors.newSingleThreadExecutor
  executor.submit(elasticsearchWriter)

  private val scheduler = Executors.newSingleThreadScheduledExecutor
  scheduler.scheduleWithFixedDelay(timeoutHandler, 1000, 1000, TimeUnit.MILLISECONDS)

  override def requestStarted(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.synchronized {
      requestsInProgress.add(rd) // keep track of in-progress request
    }
  }

  override def requestUpdated(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.synchronized {
      requestsInProgress.add(rd) // add request again to update state
    }
  }

  override def requestCompleted(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.synchronized {
      requestsInProgress.add(rd) // add request again to update state
    }
  }

  override def requestPostProcessed(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.synchronized {
      requestsInProgress.remove(rd) // stop tracking finished request
      writeQueue.add(rd) // submit finished request to be sent to elasticsearch
    }
  }

  // TODO: Use `co.elastic.clients:elasticsearch-java` client API instead of low-level REST API
  private def putElasticsearch(requestData: RequestData): Unit = {
    // requests need to be identifiable so they can be updated if they complete after they time out
    // but ids reset when geoserver restarts, so use a combination of the id and start time
    val request = new Request("PUT", s"/$index/_doc/${requestData.uid}")
    request.setJsonEntity(gson.toJson(requestData))
    restClient.performRequest(request)
  }
}

object ElasticRequestDataListener {

  val GEOSERVER_MONITOR_ELASTICSEARCH_KEY = "geomesa.geoserver.monitor.elasticsearch"

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

    clientBuilder.build
  }
}
