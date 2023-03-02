/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.plugin.source.streaming


import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.slf4j.LoggerFactory
/**
 * DirectSalesforceInputDStream
 */
class DirectSalesforceInputDStream(_ssc: StreamingContext,
                                   config: SalesforceStreamingSourceConfig,
                                   credentials: AuthenticatorCredentials
                                  ) extends InputDStream[String](_ssc) {

  private val LOG = LoggerFactory.getLogger(classOf[DirectSalesforceInputDStream])


  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[RDD[String]] = {
    LOG.info(">>>> In DirectSalesforceInputDStream compute()")
    val rdd = new SalesforceRDD(_ssc.sparkContext ,config, credentials)
    Some(rdd)
  }
}
