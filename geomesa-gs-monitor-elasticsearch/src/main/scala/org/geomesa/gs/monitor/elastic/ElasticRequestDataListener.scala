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
import org.geoserver.monitor.RequestData.Status
import org.geoserver.monitor.RequestDataListener

import java.util.concurrent.{Executors, TimeUnit}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.TimeoutException

class ElasticRequestDataListener extends RequestDataListener with LazyLogging {

  import org.geomesa.gs.monitor.elastic.ElasticRequestDataListener._

  private val config = ConfigFactory.load.getConfig(GEOSERVER_MONITOR_ELASTICSEARCH_KEY)
  private val index = config.getString("index")
  private val excludedFields =
    if (config.hasPath("excludedFields")) {
      config.getStringList("excludedFields").asScala.toSet
    } else {
      Set.empty[String]
    }

  private val gson = ExtendedRequestData.getGson(excludedFields)
  private val restClient = getRestClient(config)

  /*
   * GeoServer Request Timeout Strategy
   *
   * If a request times out, it never reaches the completed or post-processed callbacks and thus
   * would never be sent to Elasticsearch. To combat this, all requests that are in progress are
   * stored in a set ordered by their start time. Whenever a request is post-processed, it is sent
   * to Elasticsearch and removed from the set. Any in-progress requests that have exceeded the
   * configured timeout are marked as failed and sent to Elasticsearch. If a request completes
   * after it has already been marked as a timeout, the Elasticsearch index is updated using the
   * request id.
   */

  private val requestTimeout =
    if (config.hasPath("timeout")) {
      config.getLong("timeout")
    } else {
      10000L
    }

  private implicit val requestOrdering: Ordering[RequestData] = new Ordering[RequestData] {
    override def compare(rd: RequestData, rd0: RequestData): Int = {
      rd.getStartTime.getTime.compare(rd0.getStartTime.getTime)
    }
  }
  private val requestsInProgress = mutable.TreeSet.empty

  private val handleTimeouts = new Runnable {
    override def run(): Unit = {
      val timedOutRequests = new mutable.ArrayBuffer[RequestData]

      // don't read in-progress requests while they are being updated
      ElasticRequestDataListener.this.synchronized {
        val currentMillis = System.currentTimeMillis
        timedOutRequests ++= requestsInProgress.takeWhile(_.getStartTime.getTime + requestTimeout > currentMillis)
      }

      // send any timed-out requests to Elasticsearch as failures
      timedOutRequests.foreach { rd =>
        val ex = new TimeoutException("TIMEOUT")
        rd.setStatus(Status.FAILED)
        rd.setError(ex)
        rd.setErrorMessage(ex.getMessage)

        try {
          putElasticsearch(restClient, index, ExtendedRequestData(rd))
        } catch {
          case ex: Exception => logger.error(s"Failed to write request to Elasticsearch: ${ex.getMessage}")
        }
      }
    }
  }

  private val timeoutExecutor = Executors.newSingleThreadScheduledExecutor()
  timeoutExecutor.scheduleAtFixedRate(handleTimeouts, 2000, 2000, TimeUnit.MILLISECONDS)

  override def requestStarted(requestData: org.geoserver.monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    ElasticRequestDataListener.this.synchronized {
      requestsInProgress.add(rd) // keep track of in-progress requests
    }
  }

  override def requestUpdated(requestData: org.geoserver.monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    ElasticRequestDataListener.this.synchronized {
      requestsInProgress.add(rd) // add the request again because sometimes callbacks are skipped
    }
  }

  override def requestCompleted(requestData: org.geoserver.monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    ElasticRequestDataListener.this.synchronized {
      requestsInProgress.add(rd) // add the request again because sometimes callbacks are skipped
    }
  }

  override def requestPostProcessed(requestData: org.geoserver.monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    ElasticRequestDataListener.this.synchronized {
      requestsInProgress.remove(rd) // stop tracking finished requests
    }

    try {
      putElasticsearch(restClient, index, ExtendedRequestData(rd))
    } catch {
      case ex: Exception => logger.error(s"Failed to write request to Elasticsearch: ${ex.getMessage}")
    }
  }

  // TODO: Use `co.elastic.clients:elasticsearch-java` client API instead of low-level REST API
  protected def putElasticsearch(client: RestClient, index: String, requestData: RequestData): Unit = {
    // requests are stored by their id so they can be updated if they complete after they time out
    val request = new Request("PUT", s"/$index/_doc/${requestData.internalid}")
    request.setJsonEntity(gson.toJson(requestData))
    client.performRequest(request)
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
