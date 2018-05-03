/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.spark.atlas

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.annotations.VisibleForTesting
import com.hortonworks.spark.atlas.utils.Logging

abstract class AbstractEventProcessor[T: ClassTag] extends Logging {
  def conf: AtlasClientConf

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  private[atlas] val eventQueue = new LinkedBlockingQueue[T](capacity)

  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  private val eventProcessThread = new Thread {
    override def run(): Unit = {
      eventProcess()
    }
  }

  def pushEvent(event: T): Unit = {
    event match {
      case e: T =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }
      case _ => // Ignore other events
    }
  }

  def startThread(): Unit = {
    eventProcessThread.setName(this.getClass.getSimpleName + "-thread")
    eventProcessThread.setDaemon(true)
    eventProcessThread.start()
  }

  protected def process(e: T): Unit

  @VisibleForTesting
  private[atlas] def eventProcess(): Unit = {
    var stopped = false
    while (!stopped) {
      try {
        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach { e =>
          process(e)
        }
      } catch {
        case _: InterruptedException =>
          logDebug("Thread is interrupted")
          stopped = true

        case NonFatal(f) =>
          logWarn(s"Caught exception during parsing event", f)
      }
    }
  }
}
