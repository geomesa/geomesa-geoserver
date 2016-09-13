/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.security.audit

import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.metamodel.schema.{ColumnType, Schema, Table}
import org.apache.metamodel.{UpdateCallback, UpdateScript, UpdateableDataContext}
import org.geoserver.monitor.{RequestData, RequestDataListener}

/**
  * Audits requests using metamodel
  *
  * @param context metamodel data context
  * @param tableName table to write to, in the default schema.
  * @param bodyLimit number of bytes of the request body that will be stored. Use -1 to disable limit, use 0 to not store anything.
  */
class AuditRequestDataListener(context: UpdateableDataContext, tableName: String, bodyLimit: Int)
    extends RequestDataListener with LazyLogging {

  import AuditRequestDataListener._

  // create our table if it doesn't already exist
  val table = {
    val schema = context.getDefaultSchema
    val t = schema.getTableByName(tableName)
    if (t == null) {
      context.executeUpdate(createTableScript(schema, tableName))
      schema.getTableByName(tableName)
    } else {
      t
    }
  }

  logger.info(s"Auditing enabled using ${context.getClass.getSimpleName} and table ${table.getQualifiedLabel}")

  override def requestStarted(requestData: RequestData): Unit = {}

  override def requestUpdated(requestData: RequestData): Unit = {}

  override def requestCompleted(requestData: RequestData): Unit =
    context.executeUpdate(createAuditScript(table, requestData, bodyLimit))

  override def requestPostProcessed(requestData: RequestData): Unit = {}
}

object AuditRequestDataListener {

  case class Column(name: String, binding: ColumnType)

  object Columns {
    val Id =                  Column("id",                  ColumnType.STRING)
    val RequestId =           Column("requestId",           ColumnType.INTEGER)
    val Category =            Column("category",            ColumnType.STRING)
    val Path =                Column("path",                ColumnType.STRING)
    val QueryString =         Column("queryString",         ColumnType.STRING)
    val Body =                Column("body",                ColumnType.BINARY)
    val BodyContentLength =   Column("bodyContentLength",   ColumnType.INTEGER)
    val BodyContentType =     Column("bodyContentType",     ColumnType.STRING)
    val HttpMethod =          Column("httpMethod",          ColumnType.STRING)
    val StartTime =           Column("startTime",           ColumnType.DATE)
    val EndTime =             Column("endTime",             ColumnType.DATE)
    val TotalTime =           Column("totalTime",           ColumnType.INTEGER)
    val RemoteAddr =          Column("remoteAddr",          ColumnType.STRING)
    val RemoteHost =          Column("remoteHost",          ColumnType.STRING)
    val RemoteUser =          Column("remoteUser",          ColumnType.STRING)
    val RemoteUserAgent =     Column("remoteUserAgent",     ColumnType.STRING)
    val RemoteCountry =       Column("remoteCountry",       ColumnType.STRING)
    val RemoteCity =          Column("remoteCity",          ColumnType.STRING)
    val RemoteLat =           Column("remoteLat",           ColumnType.DOUBLE)
    val RemoteLon =           Column("remoteLon",           ColumnType.DOUBLE)
    val Host =                Column("host",                ColumnType.STRING)
    val InternalHost =        Column("internalHost",        ColumnType.STRING)
    val Service =             Column("service",             ColumnType.STRING)
    val Operation =           Column("operation",           ColumnType.STRING)
    val OwsVersion =          Column("owsVersion",          ColumnType.STRING)
    val SubOperation =        Column("subOperation",        ColumnType.STRING)
    val Resources =           Column("resources",           ColumnType.STRING)
    val ResponseLength =      Column("responseLength",      ColumnType.INTEGER)
    val ResponseContentType = Column("responseContentType", ColumnType.STRING)
    val ErrorMessage =        Column("errorMessage",        ColumnType.STRING)
    val Error =               Column("error",               ColumnType.STRING)
    val ResponseStatus =      Column("responseStatus",      ColumnType.INTEGER)
    val HttpReferer =         Column("httpReferer",         ColumnType.STRING)
    val Bbox =                Column("bbox",                ColumnType.STRING)
  }

  def createTableScript(schema: Schema, table: String): UpdateScript =
    new UpdateScript {
      import Columns._
      override def run(callback: UpdateCallback): Unit = {
        val update = callback.createTable(schema, table)
        update.withColumn(Id.name).ofType(Id.binding).asPrimaryKey()
        update.withColumn(RequestId.name).ofType(RequestId.binding)
        update.withColumn(Category.name).ofType(Category.binding)
        update.withColumn(Path.name).ofType(Path.binding)
        update.withColumn(QueryString.name).ofType(QueryString.binding)
        update.withColumn(Body.name).ofType(Body.binding)
        update.withColumn(BodyContentLength.name).ofType(BodyContentLength.binding)
        update.withColumn(BodyContentType.name).ofType(BodyContentType.binding)
        update.withColumn(HttpMethod.name).ofType(HttpMethod.binding)
        update.withColumn(StartTime.name).ofType(StartTime.binding)
        update.withColumn(EndTime.name).ofType(EndTime.binding)
        update.withColumn(TotalTime.name).ofType(TotalTime.binding)
        update.withColumn(RemoteAddr.name).ofType(RemoteAddr.binding)
        update.withColumn(RemoteHost.name).ofType(RemoteHost.binding)
        update.withColumn(RemoteUser.name).ofType(RemoteUser.binding)
        update.withColumn(RemoteUserAgent.name).ofType(RemoteUserAgent.binding)
        update.withColumn(RemoteCountry.name).ofType(RemoteCountry.binding)
        update.withColumn(RemoteCity.name).ofType(RemoteCity.binding)
        update.withColumn(RemoteLat.name).ofType(RemoteLat.binding)
        update.withColumn(RemoteLon.name).ofType(RemoteLon.binding)
        update.withColumn(Host.name).ofType(Host.binding)
        update.withColumn(InternalHost.name).ofType(InternalHost.binding)
        update.withColumn(Service.name).ofType(Service.binding)
        update.withColumn(Operation.name).ofType(Operation.binding)
        update.withColumn(OwsVersion.name).ofType(OwsVersion.binding)
        update.withColumn(SubOperation.name).ofType(SubOperation.binding)
        update.withColumn(Resources.name).ofType(Resources.binding)
        update.withColumn(ResponseLength.name).ofType(ResponseLength.binding)
        update.withColumn(ResponseContentType.name).ofType(ResponseContentType.binding)
        update.withColumn(ErrorMessage.name).ofType(ErrorMessage.binding)
        update.withColumn(Error.name).ofType(Error.binding)
        update.withColumn(ResponseStatus.name).ofType(ResponseStatus.binding)
        update.withColumn(HttpReferer.name).ofType(HttpReferer.binding)
        update.withColumn(Bbox.name).ofType(Bbox.binding)
        update.execute()
      }
    }

  def createAuditScript(table: Table, data: RequestData, bodyLimit: Int): UpdateScript =
    new UpdateScript {
      import Columns._
      override def run(callback: UpdateCallback): Unit = {
        val update = callback.insertInto(table)
        update.value(Id.name, UUID.randomUUID().toString)
        update.value(RequestId.name, data.getId)
        if (data.getCategory != null) { update.value(Category.name, data.getCategory) }
        if (data.getPath != null) { update.value(Path.name, data.getPath) }
        if (data.getQueryString != null) { update.value(QueryString.name, data.getQueryString) }
        if (data.getBody != null && bodyLimit != 0) {
          val body = if (bodyLimit < 0) { data.getBody } else { data.getBody.take(bodyLimit) }
          update.value(Body.name, body)
        }
        update.value(BodyContentLength.name, data.getBodyContentLength)
        if (data.getBodyContentType != null) { update.value(BodyContentType.name, data.getBodyContentType) }
        if (data.getHttpMethod != null) { update.value(HttpMethod.name, data.getHttpMethod) }
        if (data.getStartTime != null) { update.value(StartTime.name, data.getStartTime) }
        if (data.getEndTime != null) { update.value(EndTime.name, data.getEndTime) }
        update.value(TotalTime.name, data.getTotalTime)
        if (data.getRemoteAddr != null) { update.value(RemoteAddr.name, data.getRemoteAddr) }
        if (data.getRemoteHost != null) { update.value(RemoteHost.name, data.getRemoteHost) }
        if (data.getRemoteUser != null) { update.value(RemoteUser.name, data.getRemoteUser) }
        if (data.getRemoteUserAgent != null) { update.value(RemoteUserAgent.name, data.getRemoteUserAgent) }
        if (data.getRemoteCountry != null) { update.value(RemoteCountry.name, data.getRemoteCountry) }
        if (data.getRemoteCity != null) { update.value(RemoteCity.name, data.getRemoteCity) }
        update.value(RemoteLat.name, data.getRemoteLat)
        update.value(RemoteLon.name, data.getRemoteLon)
        if (data.getHost != null) { update.value(Host.name, data.getHost) }
        if (data.getInternalHost != null) { update.value(InternalHost.name, data.getInternalHost) }
        if (data.getService != null) { update.value(Service.name, data.getService) }
        if (data.getOperation != null) { update.value(Operation.name, data.getOperation) }
        if (data.getOwsVersion != null) { update.value(OwsVersion.name, data.getOwsVersion) }
        if (data.getSubOperation != null) { update.value(SubOperation.name, data.getSubOperation) }
        if (data.getResources != null) { update.value(Resources.name, data.getResources) }
        update.value(ResponseLength.name, data.getResponseLength)
        if (data.getResponseContentType != null) { update.value(ResponseContentType.name, data.getResponseContentType) }
        if (data.getErrorMessage != null) { update.value(ErrorMessage.name, data.getErrorMessage) }
        if (data.getError != null) { update.value(Error.name, data.getError) }
        if (data.getResponseStatus != null) { update.value(ResponseStatus.name, data.getResponseStatus) }
        if (data.getHttpReferer != null) { update.value(HttpReferer.name, data.getHttpReferer) }
        if (data.getBbox != null) { update.value(Bbox.name, data.getBbox.toString) }
        update.execute()
      }
    }
}