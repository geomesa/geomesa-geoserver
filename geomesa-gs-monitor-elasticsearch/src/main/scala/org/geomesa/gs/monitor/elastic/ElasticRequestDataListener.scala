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
import org.geoserver.catalog.{Catalog, ResourceInfo}
import org.geoserver.monitor
import org.geoserver.monitor.RequestData.Status
import org.geoserver.monitor.RequestDataListener
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.{ApplicationEvent, ApplicationListener}

import java.util.concurrent.{ConcurrentHashMap, Executors, LinkedBlockingQueue, TimeUnit}
import java.util.function.BiFunction
import scala.collection.JavaConverters._
import scala.concurrent.TimeoutException

class ElasticRequestDataListener(catalog: Catalog) extends RequestDataListener
  with ApplicationListener[ApplicationEvent] with LazyLogging {

  import org.geomesa.gs.monitor.elastic.ElasticRequestDataListener._

  ExtendedRequestData.catalog = catalog
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
      .filter { ms =>
        if (ms >= 1000L && ms < 10000000L) {
          true
        } else {
          logger.warn(s"Ignoring config value for timeout '$ms'")
          false
        }
      }
      .getOrElse(10000L)

  private val gson = ExtendedRequestData.getGson(excludedFields)
  private val restClient = getRestClient(config)

  /*
   * GeoServer Request Monitor Timeout Strategy
   *
   * If a request times out, it never reaches the completed or post-processed callbacks and thus
   * would never be sent to Elasticsearch. To combat this, all in-progress requests are tracked.
   * Whenever a request is post-processed, it is sent to Elasticsearch and removed from the set.
   * Any in-progress requests that exceed the configured timeout are marked as failed and sent
   * to Elasticsearch. If a request completes after it has already been marked as a timeout,
   * the Elasticsearch index is updated using the request id.
   */

  private val writeQueue = new LinkedBlockingQueue[RequestData]
  private val requestsInProgress = new ConcurrentHashMap[Long, RequestData]

  private val elasticsearchWriter = new Runnable {
    override def run(): Unit = {
      while (!Thread.interrupted) {
        val rd = writeQueue.take // block until data is available to send

        try {
          putElasticsearch(new ExtendedRequestData(rd))
          logger.info(s"Sent request '${rd.uid}' to Elasticsearch")
        } catch {
          case ex: Exception => logger.error(s"Failed to send request '${rd.uid}' to Elasticsearch: ${ex.getMessage}")
        }
      }
    }
  }

  private val timeoutHandler = new Runnable {
    override def run(): Unit = {
      val currentMillis = System.currentTimeMillis

      requestsInProgress.asScala
        .filter { case (_, rd) =>
          rd.getStartTimeMillis + timeout <= currentMillis
        }
        .foreach { case (id, _) =>
          // add timed-out request to queue if it has not already been removed
          requestsInProgress.computeIfPresent(id, new BiFunction[Long, RequestData, RequestData] {
            override def apply(id: Long, request: RequestData): RequestData = {
              logger.info(s"Request '${request.uid}' timed out")
              val ex = new TimeoutException(TIMEOUT_KEY)
              request.setStatus(Status.FAILED)
              request.setError(ex)
              request.setErrorMessage(ex.getMessage)

              writeQueue.add(request)
              null // stop tracking timed-out request
            }
          })
        }
    }
  }

  private val executor = Executors.newSingleThreadExecutor
  executor.submit(elasticsearchWriter)

  private val scheduler = Executors.newSingleThreadScheduledExecutor
  scheduler.scheduleWithFixedDelay(timeoutHandler, 1000, 1000, TimeUnit.MILLISECONDS)

  override def requestStarted(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.put(rd.internalid, rd) // keep track of in-progress request
  }

  override def requestUpdated(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.put(rd.internalid, rd) // add request again to update state
  }

  override def requestCompleted(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.put(rd.internalid, rd) // add request again to update state
  }

  override def requestPostProcessed(requestData: monitor.RequestData): Unit = {
    val rd = new RequestData(requestData)

    requestsInProgress.compute(rd.internalid, new BiFunction[Long, RequestData, RequestData] {
      override def apply(id: Long, request: RequestData): RequestData = {
        writeQueue.add(rd)
        null // stop tracking finished request
      }
    })
  }

  override def onApplicationEvent(event: ApplicationEvent): Unit = {
    event match {
      case _: ContextClosedEvent =>
        scheduler.shutdown()
        executor.shutdown()
        restClient.close()
      case _ =>
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
