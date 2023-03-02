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

import com.google.gson.Gson
import io.cdap.plugin.salesforce.SalesforceConstants
import io.cdap.plugin.salesforce.authenticator.{Authenticator, AuthenticatorCredentials}
import io.cdap.plugin.salesforce.plugin.OAuthInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.cometd.client.BayeuxClient
import org.cometd.client.transport.{ClientTransport, LongPollingTransport}
import org.cometd.common.JacksonJSONContextClient
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util
import java.util.ArrayList

/**
 * A batch-oriented interface for consuming from Salesforce
 */

class SalesforceRDD(sc: SparkContext,
                    //messages: ArrayList[String]
                    config: SalesforceStreamingSourceConfig,
                    credentials: AuthenticatorCredentials
                   ) extends RDD[String] (sc, Nil) {
  private val LOG = LoggerFactory.getLogger(classOf[SalesforceRDD])

  private val DEFAULT_PUSH_ENDPOINT = "/cometd/" + SalesforceConstants.API_VERSION

  @transient private val bayeuxClient: BayeuxClient = getClient(credentials)

  val hs = handshake()

  private val messages = new ArrayList[String]

  def handshake() {
    bayeuxClient.handshake()
    bayeuxClient.waitFor(1000, BayeuxClient.State.CONNECTED)
    LOG.info(">>>> Bayeux Client IsHandshook? :  {}", bayeuxClient.isHandshook)
    LOG.info(">>>> Bayeux Client IsConnected? : {}", bayeuxClient.isConnected)
    LOG.info(">>>> Bayeux Client IsDisconnected? : {}", bayeuxClient.isDisconnected)
    subscribe()
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    try {
      scala.collection.JavaConverters.asScalaIterator(messages.iterator())
    } catch {
      case e: IOException =>
        LOG.error("Unable to compute RDD for partition ", e)
        throw new RuntimeException("Unable to compute RDD for partition ", e)
    }
  }

  def subscribe() {
    bayeuxClient.getChannel("/topic/" + config.getPushTopicName).subscribe((channel, message) => {
      LOG.info("Message in InputDStream : {}", message)
      messages.add(new Gson().toJson(message.getDataAsMap))
      LOG.info("Message added to the list")
      LOG.info("Messages List {} ", messages)
    })
  }

  override protected def getPartitions: Array[Partition] = Array()

  @throws[Exception]
  private def getClient(credentials: AuthenticatorCredentials): BayeuxClient = {
    val oAuthInfo: OAuthInfo = Authenticator.getOAuthInfo(credentials)
    val sslContextFactory: SslContextFactory = new SslContextFactory
    // Set up a Jetty HTTP client to use with CometD
    val httpClient: HttpClient = new HttpClient(sslContextFactory)
    httpClient.setConnectTimeout(1000)
    httpClient.start()
    // Use the Jackson implementation
    val jsonContext = new JacksonJSONContextClient
    val transportOptions: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]
    transportOptions.put(ClientTransport.JSON_CONTEXT_OPTION, jsonContext)
    // Adds the OAuth header in LongPollingTransport
    val transport: LongPollingTransport = new LongPollingTransport(transportOptions, httpClient) {
      override protected def customize(exchange: Request): Unit = {
        super.customize(exchange)
        exchange.header("Authorization", "OAuth " + oAuthInfo.getAccessToken)
      }
    }
    // Now set up the Bayeux client itself
    LOG.info(">>> Bayeux client creating...")
    new BayeuxClient(oAuthInfo.getInstanceURL + DEFAULT_PUSH_ENDPOINT, transport)

  }
}
