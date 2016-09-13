/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package com.ccri.geomesa.gs.kafka09;

import org.geoserver.wps.jts.SpringBeanProcessFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeomesaKafkaProcessFactory extends SpringBeanProcessFactory {

    private static final Logger logger = LoggerFactory.getLogger(GeomesaKafkaProcessFactory.class);

    public GeomesaKafkaProcessFactory(String title, String namespace, Class markerInterface) {
        super(title, namespace, markerInterface);
        logger.info("Created GeomesaKafkaProcessFactory");
    }
}
