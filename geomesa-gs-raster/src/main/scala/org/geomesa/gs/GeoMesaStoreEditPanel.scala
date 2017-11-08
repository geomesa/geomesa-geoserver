/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs

import org.apache.wicket.AttributeModifier
import org.apache.wicket.markup.html.form.{Form, FormComponent}
import org.apache.wicket.markup.html.panel.Panel
import org.apache.wicket.model.{IModel, ResourceModel}
import org.geoserver.web.data.store.StoreEditPanel
import org.geoserver.web.data.store.panel.{CheckBoxParamPanel, ParamPanel, PasswordParamPanel, TextParamPanel}
import org.geoserver.web.util.MapModel
import org.geotools.data.DataAccessFactory.Param

abstract class GeoMesaStoreEditPanel (componentId: String, storeEditForm: Form[_])
    extends StoreEditPanel(componentId, storeEditForm) {

  def addTextPanel(paramsModel: IModel[_ <: java.util.Map[String, _]], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val textParamPanel =
      new TextParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(textParamPanel, param, resourceKey)
  }

  def addPasswordPanel(paramsModel: IModel[_ <: java.util.Map[String, _]], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val required = param.required
    val passParamPanel =
      new PasswordParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName), required)
    addPanel(passParamPanel, param, resourceKey)
  }

  def addCheckBoxPanel(paramsModel: IModel[_ <: java.util.Map[String, _]], param: Param): FormComponent[_] = {
    val paramName = param.key
    val resourceKey = getClass.getSimpleName + "." + paramName
    val checkboxPanel =
      new CheckBoxParamPanel(paramName,
        new MapModel(paramsModel, paramName).asInstanceOf[IModel[_]],
        new ResourceModel(resourceKey, paramName))
    addPanel(checkboxPanel, param, resourceKey)
  }

  def addPanel(paramPanel: Panel with ParamPanel, param: Param, resourceKey: String): FormComponent[_] = {
    paramPanel.getFormComponent.setType(classOf[String])
    val defaultTitle = String.valueOf(param.description)
    val titleModel = new ResourceModel(resourceKey + ".title", defaultTitle)
    val title = String.valueOf(titleModel.getObject)
    paramPanel.add(AttributeModifier.replace("title", title))
    add(paramPanel)
    val component = paramPanel.getFormComponent
    component.setOutputMarkupId(true)
    component
  }
}
