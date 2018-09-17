/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.catalog

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileStatus, Path, RemoteIterator}
import org.geoserver.catalog.event._
import org.geoserver.catalog._
import org.geoserver.security.decorators.DecoratingDataStore
import org.geotools.data.DataStore
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory
import org.locationtech.geomesa.fs.FileSystemDataStoreFactory.FileSystemDataStoreParams
import org.locationtech.geomesa.fs.storage.common.utils.StorageUtils
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.web.core.GeoMesaServletCatalog
import org.locationtech.geomesa.web.core.GeoMesaServletCatalog.GeoMesaLayerInfo
import org.springframework.beans.factory.InitializingBean

import scala.collection.JavaConversions._
import scala.util.Try

class GeoMesaCatalogListener extends CatalogListener with InitializingBean with LazyLogging {

  var catalog: Catalog = _

  def setCatalog(catalog: Catalog): Unit = this.catalog = catalog
  def getCatalog: Catalog = this.catalog

  override def afterPropertiesSet(): Unit = {
    logger.debug("Starting Catalog crawl for existing GeoMesa layers.")
    catalog.addListener(this)
    crawlCatalog()
    logger.debug(s"Got Catalog $catalog and added listener.")
  }

  def crawlCatalog(): Unit = catalog.getFeatureTypes.foreach(addFeatureTypeInfo)

  def addFeatureTypeInfo(featureTypeInfo: FeatureTypeInfo): Unit = {
    getStore(featureTypeInfo) match {
      case gmds: GeoMesaDataStore[_, _, _] =>

        val workspace = featureTypeInfo.getStore.getWorkspace.getName
        val layerName = featureTypeInfo.getName

        val nativeName = featureTypeInfo.getNativeName

        val sft = gmds.getSchema(nativeName)

        logger.debug(s"Registering info for layer $layerName with SFT ${sft.getTypeName} with GeoMesa Stats REST API.")
        GeoMesaServletCatalog.putGeoMesaLayerInfo(workspace, layerName, GeoMesaLayerInfo(gmds, sft))
      case s => logger.debug(s"Encountered non-GeoMesa store: ${Option(s).map(_.getClass.getName).orNull}")
    }
  }

  private def getStore(info: FeatureTypeInfo): DataStore =
    Try(unwrap(info.getStore.getDataStore(null).asInstanceOf[DataStore])).getOrElse(null)

  private def unwrap(store: DataStore): DataStore = store match {
    case ds: DecoratingDataStore => ds.unwrap(classOf[DataStore])
    case ds => ds
  }

  def removeFeatureTypeInfo(featureTypeInfo: FeatureTypeInfo): Unit = {
    getStore(featureTypeInfo) match {
      case gmds: GeoMesaDataStore[_, _, _] =>

        val workspace = featureTypeInfo.getStore.getWorkspace.getName
        val layerName = featureTypeInfo.getName

        logger.debug(s"Removing info for layer $layerName with GeoMesa Stats REST API.")

        GeoMesaServletCatalog.removeGeoMesaLayerInfo(workspace, layerName)
      case s => logger.debug(s"Encountered non-GeoMesa store: ${Option(s).map(_.getClass.getName).orNull}")
    }
  }

  override def handleAddEvent(event: CatalogAddEvent): Unit = {
    logger.debug(s"GeoMesa Catalog Listener received add event: $event of type ${event.getSource.getClass}")
    event.getSource match {
      case fti: FeatureTypeInfo => addFeatureTypeInfo(fti)
      case dti: DataStoreInfo => registerLayers(dti)
      case wsi: WorkspaceInfo => registerDataStore(wsi)
      case _ => event.getSource.getClass  // not a new layer; no action necessary.
    }
  }

  def registerDataStore(wsi: WorkspaceInfo): Unit = {
    val base = System.getProperty("GEOMESA_FSDS_BASE_DIRECTORY")
    val directory = wsi.getName

    val dsi = catalog.getFactory.createDataStore()
    dsi.setWorkspace(wsi)
    dsi.setEnabled(true)
    dsi.setName(directory)

    val factory = new FileSystemDataStoreFactory
    dsi.setType(factory.getDisplayName())
    val connParams = dsi.getConnectionParameters
    val path = s"$base$directory"
    connParams.put(FileSystemDataStoreParams.PathParam.getName, path)
    connParams.put("namespace", catalog.getNamespaceByPrefix(wsi.getName).getURI)

    catalog.save(dsi)
  }

  override def handleRemoveEvent(event: CatalogRemoveEvent): Unit = {
    logger.debug(s"GeoMesa Catalog Listener received remove event: $event")
    event.getSource match {
      case fti: FeatureTypeInfo => removeFeatureTypeInfo(fti)
      case _ => // not a new layer; no action necessary.
    }
  }

  override def handlePostModifyEvent(event: CatalogPostModifyEvent): Unit = {
    logger.debug(s"GeoMesa Catalog Listener received post modify event: $event")
  }

  override def handleModifyEvent(event: CatalogModifyEvent): Unit = {
    logger.debug(s"GeoMesa Catalog Listener received modify event: $event")

    event.getSource match {
      case fti: FeatureTypeInfo if event.getPropertyNames.contains("name") => handleLayerUpdate(event)
      case wsi: WorkspaceInfo   if event.getPropertyNames.contains("name") => handleWorkspaceUpdate(event)
      case dsi: DataStoreInfo   if event.getPropertyNames.contains("workspace") => handleDataStoreUpdate(event)
      case _ => // not a modification for a FeatureTypeInfo; ignoring
    }
  }

  // First pass of auto-registering layers.
  def registerLayers(dti: DataStoreInfo): Unit = {
    val ds = dti.getDataStore(null)
    val wsi = dti.getWorkspace

    ds.getNames.foreach { name =>
      val fs = ds.getFeatureSource(name)

      val builder: CatalogBuilder = new CatalogBuilder(catalog)
      builder.setStore(dti)
      val fti = builder.buildFeatureType(name)
      val li = builder.buildLayer(fti)
      val bbox = {
        val tmp = builder.getNativeBounds(fti)
        if (tmp.equals(ReferencedEnvelope.EVERYTHING)) {
          builder.getBoundsFromCRS(fti)
        } else {
          tmp
        }
      }

      fti.setNativeBoundingBox(bbox)
      fti.setLatLonBoundingBox(bbox)
      val namespace = catalog.getNamespaceByPrefix(wsi.getName)
      fti.setNamespace(namespace)
      catalog.add(fti)
      catalog.add(li)
    }
  }

  def handleDataStoreUpdate(event: CatalogModifyEvent): Unit = {
    event.getSource match {
      case dsi: DataStoreInfo if event.getPropertyNames.contains("workspace") =>
        val workspaceNameIdx = event.getPropertyNames.indexOf("workspace")
        val oldWorkspaceName = event.getOldValues.get(workspaceNameIdx).asInstanceOf[WorkspaceInfo].getName
        val newWorkspaceName = event.getNewValues.get(workspaceNameIdx).asInstanceOf[WorkspaceInfo].getName

        val layersForOldWorkspace = GeoMesaServletCatalog.getKeys
          .filter { case (ws, layer) => ws == oldWorkspaceName }
          .map { case (_, layer) => layer}

        layersForOldWorkspace.foreach { layer =>
          logger.debug(s"For layer $layer modifying the workspace from $oldWorkspaceName to $newWorkspaceName.")
          val gmInfo = GeoMesaServletCatalog.getGeoMesaLayerInfo(oldWorkspaceName, layer)
          GeoMesaServletCatalog.removeGeoMesaLayerInfo(oldWorkspaceName, layer)
          gmInfo.foreach { GeoMesaServletCatalog.putGeoMesaLayerInfo(newWorkspaceName, layer, _) }
        }
    }
  }

  def handleWorkspaceUpdate(event: CatalogModifyEvent): Unit = {
    event.getSource match {
      case wsi: WorkspaceInfo if event.getPropertyNames.contains("name") =>
        val workspaceNameIdx = event.getPropertyNames.indexOf("name")
        val oldWorkspaceName = event.getOldValues.get(workspaceNameIdx).asInstanceOf[String]
        val newWorkspaceName = event.getNewValues.get(workspaceNameIdx).asInstanceOf[String]

        val layersForOldWorkspace = GeoMesaServletCatalog.getKeys
          .filter { case (ws, layer) => ws == oldWorkspaceName }
          .map { case (_, layer) => layer}

        layersForOldWorkspace.foreach { layer =>
          logger.debug(s"For layer $layer modifying the workspace from $oldWorkspaceName to $newWorkspaceName.")
          val gmInfo = GeoMesaServletCatalog.getGeoMesaLayerInfo(oldWorkspaceName, layer)
          GeoMesaServletCatalog.removeGeoMesaLayerInfo(oldWorkspaceName, layer)
          gmInfo.foreach { GeoMesaServletCatalog.putGeoMesaLayerInfo(newWorkspaceName, layer, _) }
        }
    }
  }

  def handleLayerUpdate(event: CatalogModifyEvent): Unit = {
    event.getSource match {
      case fti: FeatureTypeInfo if event.getPropertyNames.contains("name")  =>
        val newLayerNameIdx = event.getPropertyNames.indexOf("name")
        val oldLayerName = event.getOldValues.get(newLayerNameIdx).asInstanceOf[String]
        val workspace = fti.getStore.getWorkspace.getName

        GeoMesaServletCatalog.getGeoMesaLayerInfo(workspace, oldLayerName).foreach { gmInfo =>
          val newLayerName = event.getNewValues.get(newLayerNameIdx).asInstanceOf[String]
          logger.debug(s"Update layername from $oldLayerName to $newLayerName in workspace $workspace.")
          GeoMesaServletCatalog.removeGeoMesaLayerInfo(workspace, oldLayerName)
          GeoMesaServletCatalog.putGeoMesaLayerInfo(workspace, newLayerName, gmInfo) }
    }
  }

  override def reloaded(): Unit = {
    logger.debug(s"GeoMesa Catalog Listener received reloaded message.")
  }
}
