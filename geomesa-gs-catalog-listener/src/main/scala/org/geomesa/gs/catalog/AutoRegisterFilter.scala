/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.catalog

import javax.servlet.http.HttpServletRequest
import javax.servlet.{FilterChain, FilterConfig, ServletRequest, ServletResponse}
import org.geoserver.catalog.Catalog
import org.geoserver.filters.GeoServerFilter
import org.springframework.beans.factory.InitializingBean
import org.geoserver.ows.Request
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

class AutoRegisterFilter extends GeoServerFilter {
  var catalog: Catalog = _
  def setCatalog(catalog: Catalog): Unit = this.catalog = catalog
  def getCatalog: Catalog = this.catalog

  override def init(filterConfig: FilterConfig): Unit = { }

  override def doFilter(servletRequest: ServletRequest, servletResponse: ServletResponse, filterChain: FilterChain): Unit = {
    val request = new Request()
    request.setHttpRequest(servletRequest.asInstanceOf[HttpServletRequest])

    val path = servletRequest.asInstanceOf[HttpServletRequest].getServletPath.split("/")
    if (path.length > 1) {
      createWorkspace(path(1))
    }
    filterChain.doFilter(servletRequest, servletResponse)
  }

  def createWorkspace(ws: String): Unit = {
    val wsi = catalog.getWorkspaceByName(ws)
    if (wsi == null && shouldCreate(ws)) {
      val newWorkspace = catalog.getFactory.createWorkspace()
      newWorkspace.setName(ws)

      val namespace = catalog.getFactory.createNamespace()
      namespace.setPrefix(newWorkspace.getName)
      namespace.setURI(s"http://geomesa.org/$ws")

      catalog.add(namespace)
      catalog.add(newWorkspace)
    }
  }

  def shouldCreate(workspace: String): Boolean = {
    // Check that
    // val base = System.getProperty("GEOMESA_FSDS_BASE_DIRECTORY") + workspace exists!
    !Seq("gwc", "styles", "web", "index.html", "openlayers3").contains(workspace)
  }

  override def destroy(): Unit = { }
}
