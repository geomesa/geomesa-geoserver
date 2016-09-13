/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package org.geomesa.gs.wps.kafka10

import org.geotools.process.factory.AnnotatedBeanProcessFactory
import org.geotools.text.Text

class KafkaProcessFactory extends AnnotatedBeanProcessFactory(Text.text("GeoMesa Kafka Process Factory"), "kafka")