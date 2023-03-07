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
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.cometd.client.BayeuxClient
import org.cometd.client.transport.{ClientTransport, LongPollingTransport}
import org.cometd.common.JacksonJSONContextClient
import org.eclipse.jetty.client.HttpClient
import org.eclipse.jetty.client.api.Request
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.slf4j.LoggerFactory

import java.util
import java.util.concurrent.{ConcurrentMap, LinkedBlockingQueue}
/**
 * DirectSalesforceInputDStream
 */
class DirectSalesforceInputDStream(_ssc: StreamingContext,
                                    config: SalesforceStreamingSourceConfig,
                                    credentials: AuthenticatorCredentials,
                                    dataMap: ConcurrentMap[String, Integer]
                                  ) extends InputDStream[String](_ssc) {

  private val LOG = LoggerFactory.getLogger(classOf[DirectSalesforceInputDStream])

  private val DEFAULT_PUSH_ENDPOINT = "/cometd/" + SalesforceConstants.API_VERSION

  var bayeuxClient: BayeuxClient = _

  //private val messages = new util.ArrayList[String]
  private val messages = new LinkedBlockingQueue[String]

  override def start(): Unit = {
    try {
      bayeuxClient = getClient(credentials)
      bayeuxClient.handshake()
      bayeuxClient.waitFor(1000, BayeuxClient.State.CONNECTED)
      LOG.info(">>>> Bayeux Client IsHandshook? :  {}", bayeuxClient.isHandshook)
      LOG.info(">>>> Bayeux Client IsConnected? : {}", bayeuxClient.isConnected)
      LOG.info(">>>> Bayeux Client IsDisconnected? : {}", bayeuxClient.isDisconnected)
      // Register Replay Extension with Bayeux Client
      bayeuxClient.addExtension(new ReplayExtension(dataMap))
      subscribe()
    } catch {
      case e: Exception =>
        LOG.error("Error streaming from Salesforce", e)
    }
  }

  override def stop(): Unit = {
    // Stop the Salesforce Streaming connector
    // ...
    //bayeuxClient.disconnect()
  }

  override def compute(validTime: Time): Option[RDD[String]] = {
    LOG.info(">>>> In DirectSalesforceInputDStream compute()")
    // Get the streaming data for the given time

    var message: String = ""
    /************************* Spark not active issue logging *************************/
    /*LOG.info("Spark Context Status [BEFORE STOPPING]: {}", _ssc.sparkContext.isStopped)
    _ssc.sparkContext.stop()
    LOG.info("Spark Context Status [AFTER STOPPING]: {}", _ssc.sparkContext.isStopped)
    if (_ssc.sparkContext.isStopped) {
      LOG.info("Spark Context Status [BEFORE STARTING]: {}", _ssc.sparkContext.isStopped)
      _ssc.start()
      _ssc.awaitTermination()
      LOG.info("Spark Context Status [AFTER STARTING]: {}", _ssc.sparkContext.isStopped)
    }*/

    /*************** InputDStream approach ****************************/
    /*val rdd = Some(_ssc.sparkContext.parallelize(List.apply(messages)))
    rdd*/

    /*******************************************************/
    var rdd = Some(_ssc.sparkContext.parallelize(List("")))
    if (!messages.isEmpty) {
      message = messages.poll()
      LOG.info("Message retrieved from the Queue: {}", message)
      // Convert the streaming data to an RDD and return it
      rdd = Some(_ssc.sparkContext.parallelize(List(message)))
    } else {
      rdd = Some(_ssc.sparkContext.emptyRDD)
    }
    rdd
    /*******************************************************/


    /***************** RDD approach **********************************/
    /*val rdd = new SalesforceRDD(_ssc.sparkContext ,config, credentials)
    Some(rdd)*/

    /*val rdd: Option[RDD[String]] = Some (new SalesforceRDD(_ssc.sparkContext, config, credentials))
    rdd*/
  }

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
    new BayeuxClient(oAuthInfo.getInstanceURL + DEFAULT_PUSH_ENDPOINT, transport)
  }

  def subscribe() {
    bayeuxClient.getChannel("/topic/" + config.getPushTopicName).subscribe((channel, message) => {
      //def foo(channel: ClientSessionChannel, message: Message) = {
      LOG.info("Message in InputDStream : {}", message)
      messages.add(new Gson().toJson(message.getDataAsMap))
      LOG.info("Message added to the queue. Message Queue : {} ", messages)
    })
  }
}

