/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package com.ccri.geomesa.gs.security

import java.io.Serializable
import java.util

import org.apache.accumulo.core.security.Authorizations
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.springframework.security.authentication.TestingAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder

class TestAuthorizationsProvider extends AuthorizationsProvider {

  override def getAuthorizations: java.util.List[String] = {
    import scala.collection.JavaConversions._
    val authentication = SecurityContextHolder.getContext.getAuthentication.asInstanceOf[TestingAuthenticationToken]
    authentication.getAuthorities.map(_.getAuthority).toSeq
  }

  override def configure(params: util.Map[String, Serializable]): Unit = {}
}
