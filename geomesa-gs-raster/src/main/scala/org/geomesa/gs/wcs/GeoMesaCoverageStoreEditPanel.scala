/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wcs

import org.apache.wicket.markup.html.form.validation.IFormValidator
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.model.PropertyModel
import org.geomesa.gs.GeoMesaStoreEditPanel
import org.geoserver.catalog.CoverageStoreInfo
import org.locationtech.geomesa.raster.wcs.AccumuloUrl

class GeoMesaCoverageStoreEditPanel(componentId: String, storeEditForm: Form[_])
  extends GeoMesaStoreEditPanel(componentId, storeEditForm) {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  private val model = storeEditForm.getModel
  setDefaultModel(model)

  private val storeInfo = storeEditForm.getModelObject.asInstanceOf[CoverageStoreInfo]
  storeInfo.getConnectionParameters.putAll(parseConnectionParametersFromURL(storeInfo.getURL))

  private val paramsModel = new PropertyModel(model, "connectionParameters")
  private val instanceId = addTextPanel(paramsModel, InstanceIdParam)
  private val zookeepers = addTextPanel(paramsModel, ZookeepersParam)
  private val user = addTextPanel(paramsModel, UserParam)
  private val password = addPasswordPanel(paramsModel, PasswordParam)
  private val auths = addTextPanel(paramsModel, AuthsParam)
  private val visibilities = addTextPanel(paramsModel, VisibilitiesParam)
  private val tableName = addTextPanel(paramsModel, CatalogParam)
  private val collectQueryStats = addCheckBoxPanel(paramsModel, AuditQueriesParam)

  storeEditForm.add(new IFormValidator() {
    override def getDependentFormComponents: Array[FormComponent[_]] =
      Array(instanceId, zookeepers, user, password, auths, visibilities, tableName, collectQueryStats)

    override def validate(form: Form[_]): Unit = {
      val storeInfo = form.getModelObject.asInstanceOf[CoverageStoreInfo]
      val accumuloUrl = AccumuloUrl(user.getValue, password.getValue, instanceId.getValue,
        zookeepers.getValue, tableName.getValue, Some(auths.getValue).filterNot(_.isEmpty),
        Some(visibilities.getValue).filterNot(_.isEmpty), java.lang.Boolean.valueOf(collectQueryStats.getValue))
      storeInfo.setURL(accumuloUrl.url)
    }
  })

  private def parseConnectionParametersFromURL(url: String): java.util.Map[String, String] = {
    val params = new java.util.HashMap[String, String]
    if (url != null && url.startsWith("accumulo:")) {
      val parsed = AccumuloUrl(url)
      params.put(UserParam.key, parsed.user)
      params.put(PasswordParam.key, parsed.password)
      params.put(InstanceIdParam.key, parsed.instanceId)
      params.put(CatalogParam.key, parsed.table)
      params.put(ZookeepersParam.key, parsed.zookeepers)
      params.put(AuthsParam.key, parsed.auths.getOrElse(""))
      params.put(VisibilitiesParam.key, parsed.visibilities.getOrElse(""))
      params.put(AuditQueriesParam.key, parsed.collectStats.toString)
    }
    params
  }
}
