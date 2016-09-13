/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials are
 * made available under the terms of the GNU GENERAL PUBLIC LICENSE,
 * Version 2 which accompanies this distribution and is available at
 * https://opensource.org/licenses/GPL-2.0.
 ***********************************************************************/

package com.ccri.geomesa.gs.kafka09

import java.util.concurrent.ScheduledThreadPoolExecutor

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.springframework.context.support.ClassPathXmlApplicationContext
import org.springframework.scheduling.concurrent.ScheduledExecutorTask

@RunWith(classOf[JUnitRunner])
class KafkaPluginSpringTest extends Specification {

  "Spring" should {
    "Work with the Kafka Processes" in {
      val ctx = new ClassPathXmlApplicationContext("kafkaPluginTestApplicationContext.xml","applicationContext.xml")
      ctx.isActive must beTrue
      ctx.getBean("replayKafkaLayerReaper") must beAnInstanceOf[ReplayKafkaLayerReaperProcess]
      ctx.getBean("replayKafkaDataStoreProcess") must beAnInstanceOf[ReplayKafkaDataStoreProcess]
      ctx.getBean("replayKafkaCleanupTask") must beAnInstanceOf[ScheduledExecutorTask]
      ctx.getBean("replayKafkaCleanupTaskFactory") must beAnInstanceOf[ScheduledThreadPoolExecutor]
    }
  }
}