/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wfs.output

import com.typesafe.scalalogging.LazyLogging
import net.opengis.wfs.{GetFeatureType => GetFeatureTypeV1, QueryType => QueryTypeV1}
import net.opengis.wfs20.{GetFeatureType => GetFeatureTypeV2, QueryType => QueryTypeV2}
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.util.Version
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.conf.QueryHints._
import org.locationtech.geomesa.index.utils.bin.BinSorter
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.bin.{AxisOrder, BinaryOutputEncoder}
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

import java.io.{BufferedOutputStream, OutputStream}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, Executors}
import javax.xml.namespace.QName

/**
 * Output format for wfs requests that encodes features into a binary format.
 * To trigger, use outputFormat=application/vnd.binary-viewer in your wfs request
 *
 * Required flags:
 * format_options=trackId:<track_attribute_name>;
 *
 * Optional flags:
 * format_options=trackId:<track_attribute_name>;geom:<geometry_attribute_name>;dtg:<dtg_attribute_name>;label:<label_attribute_name>
 *
 * @param geoServer handle to geoserver
 */
class BinaryViewerOutputFormat(geoServer: GeoServer)
    extends WFSGetFeatureOutputFormat(geoServer, BinaryViewerOutputFormat.OutputFormatStrings)
        with FormatOptionsCallback with LazyLogging {

  import BinaryViewerOutputFormat._

  import scala.collection.JavaConverters._

  override def getMimeType(value: AnyRef, operation: Operation): String = MIME_TYPE

  override def getCapabilitiesElementName: String = "BIN"

  override def getPreferredDisposition(value: AnyRef, operation: Operation): String = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation): String = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    // if they have requested a label, then it will be 24 byte encoding (assuming the field exists...)
    val size = if (gfr.getFormatOptions.containsKey(LABEL_FIELD)) { "24" } else { "16" }
    s"$name.$FILE_EXTENSION$size"
  }

  override def write(
      featureCollections: FeatureCollectionResponse,
      output: OutputStream,
      getFeature: Operation): Unit = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val request: GetFeatureRequest = GetFeatureRequest.adapt(getFeature.getParameters()(0))

    // whether to do final merge sorting here or let the caller handle it
    val mergeSort =
      Option(request.getFormatOptions.get(SORT_FIELD)).exists(_.toString.toBoolean) ||
          sys.props.getOrElse(SORT_SYS_PROP, DEFAULT_SORT).toBoolean

    val bos = new BufferedOutputStream(output)
    featureCollections.getFeatures.asScala.foreach { fc =>
      WithClose(CloseableIterator(fc.asInstanceOf[SimpleFeatureCollection].features())) { iter =>
        val schema = fc.asInstanceOf[SimpleFeatureCollection].getSchema
        val aggregated = schema == BinaryOutputEncoder.BinEncodedSft
        if (aggregated) {
          // encodings have already been computed
          val aggregates = iter.map(_.getAttribute(BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]])
          if (mergeSort) {
            sort(request, aggregates, bos)
          } else {
            // no sort, just write directly to the output
            aggregates.foreach(bos.write)
          }
        } else {
          logger.warn(s"Server side bin aggregation is not enabled for feature collection '${fc.getClass}'")
          val hints = new Hints()
          populateFormatOptions(request, hints)
          // for non-geomesa fs we do the encoding here
          val trackIndex = Some(hints.getBinTrackIdField).map(schema.indexOf).filter(_ != -1)
          val geomIndex = hints.getBinGeomField.map(schema.indexOf).filter(_ != -1)
          val dtgIndex = hints.getBinDtgField.map(schema.indexOf).filter(_ != -1)
          val labelIndex = hints.getBinLabelField.map(schema.indexOf).filter(_ != -1)
          // depending on srs requested and wfs versions, axis order can be flipped
          val axisOrder = checkAxisOrder(getFeature)
          val options = EncodingOptions(geomIndex, dtgIndex, trackIndex, labelIndex, Some(axisOrder))
          BinaryOutputEncoder(schema, options).encode(iter, bos, mergeSort)
        }
      }
    }
    // none of the implementations in geoserver call 'close' on the output stream
    bos.flush()
  }

  override protected def populateFormatOptions(request: GetFeatureRequest, hints: Hints): Unit = {
    val options = request.getFormatOptions
    hints.put(BIN_TRACK, Option(options.get(TRACK_ID_FIELD)).map(_.toString).getOrElse {
      throw new IllegalArgumentException(s"$TRACK_ID_FIELD is a required format option")
    })
    Option(options.get(GEOM_FIELD)).map(_.toString).foreach(hints.put(BIN_GEOM, _))
    Option(options.get(DATE_FIELD)).map(_.toString).foreach(hints.put(BIN_DTG, _))
    Option(options.get(LABEL_FIELD)).map(_.toString).foreach(hints.put(BIN_LABEL, _))

    hints.put(BIN_SORT,
      Option(options.get(SORT_FIELD).asInstanceOf[String]).exists(_.toBoolean) ||
          sys.props.getOrElse(SORT_SYS_PROP, DEFAULT_SORT).toBoolean ||
          sys.props.getOrElse(PARTIAL_SORT_SYS_PROP, DEFAULT_SORT).toBoolean)

    hints.put(BIN_BATCH_SIZE, sys.props.getOrElse(BATCH_SIZE_SYS_PROP, DEFAULT_BATCH_SIZE).toInt)
  }

  /**
   * Perform a merge sort on the aggregated results
   *
   * @param request request
   * @param aggregates aggregated partial results
   * @param os output stream
   */
  private def sort(request: GetFeatureRequest, aggregates: Iterator[Array[Byte]], os: OutputStream): Unit = {
    val binSize = if (request.getFormatOptions.get(LABEL_FIELD) == null) { 16 } else { 24 }
    // we do some asynchronous pre-merging while we are waiting for all the data to come in
    // the pre-merging is expensive, as it merges in memory
    // the final merge doesn't have to allocate space for merging, as it writes directly to the output
    val numThreads = sys.props.getOrElse(SORT_THREADS_SYS_PROP, DEFAULT_SORT_THREADS).toInt
    val executor = Executors.newFixedThreadPool(numThreads)
    // access to this is manually synchronized so we can pull off 2 items at once
    val mergeQueue = collection.mutable.PriorityQueue.empty[Array[Byte]](new Ordering[Array[Byte]] {
      // shorter first
      override def compare(x: Array[Byte], y: Array[Byte]): Int = y.length.compareTo(x.length)
    })
    // holds buffers we don't want to consider anymore due to there size - also manually synchronized
    val doneMergeQueue = collection.mutable.ArrayBuffer.empty[Array[Byte]]
    val maxSizeToMerge = sys.props.getOrElse(SORT_HEAP_SYS_PROP, DEFAULT_SORT_HEAP).toInt
    val latch = new CountDownLatch(numThreads)
    val keepMerging = new AtomicBoolean(true)
    var i = 0
    while (i < numThreads) {
      executor.submit(new Runnable() {
        override def run(): Unit = {
          while (keepMerging.get()) {
            // pull out the 2 smallest items to merge
            // the final merge has to compare the first item in each buffer
            // so reducing the number of buffers helps
            val (left, right) = mergeQueue.synchronized {
              if (mergeQueue.length > 1) {
                (mergeQueue.dequeue(), mergeQueue.dequeue())
              } else {
                (null, null)
              }
            }
            if (left != null) { // right will also not be null
              if (right.length > maxSizeToMerge) {
                if (left.length > maxSizeToMerge) {
                  doneMergeQueue.synchronized(doneMergeQueue.append(left, right))
                } else {
                  doneMergeQueue.synchronized(doneMergeQueue.append(right))
                  mergeQueue.synchronized(mergeQueue.enqueue(left))
                }
                Thread.sleep(10)
              } else {
                val result = BinSorter.mergeSort(left, right, binSize)
                mergeQueue.synchronized(mergeQueue.enqueue(result))
              }
            } else {
              // if we didn't find anything to merge, wait a bit before re-checking
              Thread.sleep(10)
            }
          }
          latch.countDown() // indicate this thread is done
        }
      })
      i += 1
    }
    // queue up the aggregates coming in so that they can be processed by the merging threads above
    aggregates.foreach(a => mergeQueue.synchronized(mergeQueue.enqueue(a)))
    // once all data is back from the tservers, stop pre-merging and start streaming back to the client
    keepMerging.set(false)
    executor.shutdown() // this won't stop the threads, but will cleanup once they're done
    latch.await() // wait for the merge threads to finish
    // get an iterator that returns in sorted order
    val bins = BinSorter.mergeSort((doneMergeQueue ++ mergeQueue).iterator, binSize)
    while (bins.hasNext) {
      val (aggregate, offset) = bins.next()
      os.write(aggregate, offset, binSize)
    }
  }
}

object BinaryViewerOutputFormat extends LazyLogging {

  import scala.collection.JavaConverters._

  val MIME_TYPE = "application/vnd.binary-viewer"
  val FILE_EXTENSION = "bin"
  val TRACK_ID_FIELD = "TRACKID"
  val GEOM_FIELD     = "GEOM"
  val DATE_FIELD     = "DTG"
  val LABEL_FIELD    = "LABEL"
  val SORT_FIELD     = "SORT"

  val SORT_SYS_PROP         = "geomesa.output.bin.sort"
  val PARTIAL_SORT_SYS_PROP = "geomesa.output.bin.sort.partial"
  val SORT_THREADS_SYS_PROP = "geomesa.output.bin.sort.threads"
  val SORT_HEAP_SYS_PROP    = "geomesa.output.bin.sort.memory"
  val BATCH_SIZE_SYS_PROP   = "geomesa.output.bin.batch.size"

  val DEFAULT_SORT = "false"
  val DEFAULT_SORT_THREADS = "2"
  val DEFAULT_SORT_HEAP = "2097152" // 2MB
  val DEFAULT_BATCH_SIZE = "65536" // 1MB for 16 byte bins

  // constants used to determine axis order from geoserver
  val wfsVersion1 = new Version("1.0.0")
  val srsVersionOnePrefix = "http://www.opengis.net/gml/srs/epsg.xml#"
  val srsVersionOnePlusPrefix = "urn:x-ogc:def:crs:epsg:"
  val srsNonStandardPrefix = "epsg:"

  val OutputFormatStrings: java.util.Set[String] = Set("bin", BinaryViewerOutputFormat.MIME_TYPE).asJava

  /**
   * Determines the order of lat/lon in simple features returned by this request.
   *
   * See http://docs.geoserver.org/2.5.x/en/user/services/wfs/basics.html#axis-ordering for details
   * on how geoserver handles axis order.
   *
   * @param getFeature operation
   * @return
   */
  def checkAxisOrder(getFeature: Operation): AxisOrder.AxisOrder =
    getSrs(getFeature) match {
      // if an explicit SRS is requested, that takes priority
      // SRS format associated with WFS 1.1.0 and 2.0.0 - lat is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePlusPrefix) => AxisOrder.LatLon
      // SRS format associated with WFS 1.0.0 - lon is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePrefix) => AxisOrder.LonLat
      // non-standard SRS format - geoserver puts lon first
      case Some(srs) if srs.toLowerCase.startsWith(srsNonStandardPrefix) => AxisOrder.LonLat
      case Some(srs) =>
        val valid = s"${srsVersionOnePrefix}xxxx, ${srsVersionOnePlusPrefix}xxxx, ${srsNonStandardPrefix}xxxx"
        throw new IllegalArgumentException(s"Invalid SRS format: '$srs'. Valid options are: $valid")
      // if no explicit SRS: wfs 1.0.0 stores x = lon y = lat, anything greater stores x = lat y = lon
      case None => if (getFeature.getService.getVersion.compareTo(wfsVersion1) > 0) AxisOrder.LatLon else AxisOrder.LonLat
    }

  def getTypeName(getFeature: Operation): Option[QName] = {
    val typeNamesV2 = getFeatureTypeV2(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeNames.asScala.toSeq)
        .getOrElse(Seq.empty)
    val typeNamesV1 = getFeatureTypeV1(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeName.asScala.toSeq)
        .getOrElse(Seq.empty)
    val typeNames = typeNamesV2 ++ typeNamesV1
    if (typeNames.lengthCompare(1) > 0) {
      logger.warn(s"Multiple TypeNames detected in binary format request (using first): $typeNames")
    }
    typeNames.headOption.map(_.asInstanceOf[QName])
  }

  /**
   * Function to pull requested SRS out of a WFS request
   *
   * @param getFeature operation
   * @return
   */
  def getSrs(getFeature: Operation): Option[String] =
    getFeatureTypeV2(getFeature).flatMap(getSrs)
        .orElse(getFeatureTypeV1(getFeature).flatMap(getSrs))

  /**
   * Function to pull requested SRS out of WFS 1.0.0/1.1.0 request
   *
   * @param getFeatureType operation
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV1): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   * Function to pull requested SRS out of WFS 2 request
   *
   * @param getFeatureType operation
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV2): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   *
   * @param getFeature operation
   * @return
   */
  def getFeatureTypeV2(getFeature: Operation): Option[GetFeatureTypeV2] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV2])
        .map(_.asInstanceOf[GetFeatureTypeV2])

  /**
   *
   * @param getFeature operation
   * @return
   */
  def getFeatureTypeV1(getFeature: Operation): Option[GetFeatureTypeV1] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV1])
        .map(_.asInstanceOf[GetFeatureTypeV1])

  /**
   * Pull out query object from request
   *
   * @param getFeatureType operation
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV1): Option[QueryTypeV1] =
    getFeatureType.getQuery.iterator().asScala.collectFirst {
      case q: QueryTypeV1 => q
    }

  /**
   * Pull out query object from request
   *
   * @param getFeatureType operation
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV2): Option[QueryTypeV2] =
    getFeatureType.getAbstractQueryExpressionGroup.iterator().asScala.collectFirst {
      case q: QueryTypeV2 => q
    }
}
