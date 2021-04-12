/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.rest.kafka

import com.typesafe.scalalogging.LazyLogging
import org.geoserver.rest.{RestBaseController, RestException}
import org.locationtech.geomesa.kafka.data.KafkaCacheLoader
import org.springframework.http.{HttpStatus, MediaType}
import org.springframework.web.bind.annotation.{GetMapping, RequestMapping, RestController}

@RestController
@RequestMapping(path = Array("/rest/kafka"),  produces = Array(MediaType.APPLICATION_JSON_VALUE))
class KafkaLoaderStatusController extends RestBaseController with LazyLogging {
  @GetMapping
  def statusGet(): String = {
    if (KafkaCacheLoader.LoaderStatus.allLoaded()) {
      "ok"
    } else {
      throw new RestException("KDS Layers still performing initial load.", HttpStatus.SERVICE_UNAVAILABLE)
    }
  }
}
