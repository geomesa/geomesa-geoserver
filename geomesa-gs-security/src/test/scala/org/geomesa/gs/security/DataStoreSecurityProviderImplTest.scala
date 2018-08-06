/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.security

import org.geotools.data._
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.data.simple.{SimpleFeatureReader, SimpleFeatureSource, SimpleFeatureStore}
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.feature.simple.SimpleFeatureImpl
import org.geotools.feature.{DefaultFeatureCollection, FeatureCollection}
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.security._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class DataStoreSecurityProviderImplTest extends Specification with Mockito {

  sequential

  SecurityContextHolder.setStrategyName(SecurityContextHolder.MODE_INHERITABLETHREADLOCAL)
  val ctx = SecurityContextHolder.createEmptyContext()
  ctx.setAuthentication(new TestingAuthenticationToken(null, null, "USER"))
  SecurityContextHolder.setContext(ctx)
  System.setProperty(AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY, classOf[TestAuthorizationsProvider].getName)

  val testSFT = SimpleFeatureTypes.createType("test", "name:String,*geom:Point:srid=4326")

  val features: Seq[SimpleFeature] = Seq(null, "USER", "ADMIN", "USER&ADMIN", "USER|ADMIN").zipWithIndex.map { case (vis, i) =>
    val sf = new SimpleFeatureImpl(Array.ofDim[AnyRef](2), testSFT, new FeatureIdImpl(i.toString), false)
    sf.visibility = vis
    sf
  }


  "DataStoreSecurityProviderImpl" should {

    "be able to secure a feature reader " in {
      val fr = new DelegateFeatureReader[SimpleFeatureType, SimpleFeature](testSFT, new DelegateFeatureIterator[SimpleFeature](features.iterator))
      
      val secureFr = new DataStoreSecurityProviderImpl().secure(fr)

      secureFr.hasNext must beTrue
      secureFr.next mustEqual features(1)

      secureFr.hasNext must beTrue
      secureFr.next mustEqual features(4)

      secureFr.hasNext must beFalse
    }

    "be able to secure a feature collection" in {
      val fc = new DefaultFeatureCollection(null, testSFT)
      fc.addAll(features)

      validate(new DataStoreSecurityProviderImpl().secure(fc))
    }

    "be able to secure a feature source when getting all features" in {
      val fc = new DefaultFeatureCollection(null, testSFT)
      fc.addAll(features)

      val ds = mock[DataStore]

      val fs = mock[SimpleFeatureSource]
      fs.getDataStore returns ds

      val secureFs = new DataStoreSecurityProviderImpl().secure(fs)

      fs.getFeatures returns fc

      validate(secureFs.getFeatures)
    }

    "be able to secure a feature source when using a query" in {
      val fc = new DefaultFeatureCollection(null, testSFT)
      fc.addAll(features)

      val ds = mock[DataStore]

      val fs = mock[SimpleFeatureSource]
      fs.getDataStore returns ds

      val secureFs = new DataStoreSecurityProviderImpl().secure(fs)

      val query = mock[Query]
      fs.getFeatures(query) returns fc

      validate(secureFs.getFeatures(query))
    }

    "be able to secure a feature source when using a filter" in {
      val fc = new DefaultFeatureCollection(null, testSFT)
      fc.addAll(features)

      val ds = mock[DataStore]

      val fs = mock[SimpleFeatureSource]
      fs.getDataStore returns ds

      val secureFs = new DataStoreSecurityProviderImpl().secure(fs)

      val filter = mock[Filter]
      fs.getFeatures(filter) returns fc

      validate(secureFs.getFeatures(filter))
    }

    "return the secure DataStore" in {
      val sfs = mock[SimpleFeatureSource]
      sfs.getSchema returns testSFT

      val secureDS = mock[GMSecureDataStore]

      val secureSource = new GMSecureFeatureSource(sfs, secureDS)
      secureSource.getDataStore mustEqual secureDS
    }

    "return the secure DataAccess" in {
      val fs = mock[FeatureSource[SimpleFeatureType, SimpleFeature]]
      fs.getSchema returns testSFT

      val secureDA = mock[GMSecureDataAccess]

      val secureSource = GMSecureFeatureSource(fs, secureDA)
      secureSource.getDataStore mustEqual secureDA
    }

    "or create a secure DataStore" in {
      val ds = mock[DataStore]
      val fs = mock[SimpleFeatureStore]
      fs.getSchema returns testSFT
      fs.getDataStore returns ds

      val secureFs = GMSecureFeatureSource(fs)
      secureFs.getDataStore.isInstanceOf[GMSecureDataStore] must beTrue
    }

    "or create a secure DataAccess" in {
      val da = mock[DataAccess[SimpleFeatureType, SimpleFeature]]
      val fs = mock[FeatureSource[SimpleFeatureType, SimpleFeature]]
      fs.getSchema returns testSFT
      fs.getDataStore returns da

      val secureFs = GMSecureFeatureSource(fs)
      secureFs.getDataStore.isInstanceOf[GMSecureDataAccess] must beTrue
    }

    "provide a secure feature source" in {
      val name = mock[Name]
      val fs = mock[SimpleFeatureStore]

      val da = mock[DataAccess[SimpleFeatureType, SimpleFeature]]
      da.getFeatureSource(name) returns fs

      val secureDa = new GMSecureDataAccess(da)

      val result = secureDa.getFeatureSource(name)
      result.isInstanceOf[GMSecureFeatureSource] must beTrue
      result.getDataStore mustEqual secureDa
    }

    "provide a secure feature source by Name" in {
      val fs = mock[SimpleFeatureStore]
      val fr = mock[SimpleFeatureReader]

      val ds = mock[DataStore]
      val secureDs = new GMSecureDataStore(ds)

      val name = mock[Name]
      ds.getFeatureSource(name) returns fs

      val result = secureDs.getFeatureSource(name)
      result.isInstanceOf[GMSecureFeatureSource] must beTrue
      result.getDataStore mustEqual secureDs
    }

    "provide a secure feature source by String" in {
      val fs = mock[SimpleFeatureStore]
      val fr = mock[SimpleFeatureReader]

      val ds = mock[DataStore]
      val secureDs = new GMSecureDataStore(ds)

      val name = "test"
      ds.getFeatureSource(name) returns fs

      val result = secureDs.getFeatureSource(name)
      result.isInstanceOf[GMSecureFeatureSource] must beTrue
      result.getDataStore mustEqual secureDs
    }

    "provide a secure feature reader" in {
      val fs = mock[SimpleFeatureStore]
      val fr = mock[SimpleFeatureReader]

      val ds = mock[DataStore]
      val secureDs = new GMSecureDataStore(ds)

      val query = mock[Query]
      val txn = mock[Transaction]
      ds.getFeatureReader(query, txn) returns fr

      val result = secureDs.getFeatureReader(query, txn)
      result.isInstanceOf[FilteringFeatureReader[SimpleFeatureType, SimpleFeature]] must beTrue
      result.asInstanceOf[FilteringFeatureReader[SimpleFeatureType, SimpleFeature]].getDelegate mustEqual fr
    }
  }

  def validate(secureFc: FeatureCollection[SimpleFeatureType, SimpleFeature]): MatchResult[Boolean] = {
    val iter = secureFc.features()

    iter.hasNext must beTrue
    iter.next mustEqual features(1)

    iter.hasNext must beTrue
    iter.next mustEqual features(4)

    iter.hasNext must beFalse
  }
}
