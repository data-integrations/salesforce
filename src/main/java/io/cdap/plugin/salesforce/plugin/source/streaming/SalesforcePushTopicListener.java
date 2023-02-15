/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

package io.cdap.plugin.salesforce.plugin.source.streaming;

import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.cometd.common.JSONContext;
import org.cometd.common.JacksonJSONContextClient;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Listens to a specific Salesforce pushTopic and adds messages to the blocking queue,
 * which can be read by a user of the class.
 */
public class SalesforcePushTopicListener {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforcePushTopicListener.class);

  private static final String DEFAULT_PUSH_ENDPOINT = "/cometd/" + SalesforceConstants.API_VERSION;
  /**
   * Timeout of 110 seconds is enforced by Salesforce Streaming API and is not configurable.
   * So we enforce the same on client.
   */
  private static final long CONNECTION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);
  private static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);

  private static final int HANDSHAKE_CHECK_INTERVAL_MS = 1000;

  // store message string not JSONObject, since it's not serializable for later Spark usage
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>();

  private final AuthenticatorCredentials credentials;
  private final String topic;
  private ConcurrentMap<String, Integer> dataMap;
  private BayeuxClient bayeuxClient;

  private JSONContext.Client jsonContext;

  public SalesforcePushTopicListener(AuthenticatorCredentials credentials, String topic,
                                     ConcurrentMap<String, Integer> dataMap) {
    this.credentials = credentials;
    this.topic = topic;
    this.dataMap = dataMap;
  }

  /**
   * Start the Bayeux Client which listens to the Salesforce PushTopic and saves received messages
   * to the queue.
   */
  public void start() {
    try {
      createSalesforceListener();
      waitForHandshake();
      subscribe();
    } catch (Exception e) {
      throw new RuntimeException("Could not start client", e);
    }
  }

  /**
   * Retrieves message from the messages queue, waiting up to the
   * specified wait time if necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up
   * @param unit    timeunit of timeout
   * @return the message, or {@code null} if the specified
   * waiting time elapses before an element is available
   * @throws InterruptedException blocking call is interrupted
   */
  public String getMessage(long timeout, TimeUnit unit) throws InterruptedException {
    return messagesQueue.poll(timeout, unit);
  }

  private BayeuxClient getClient(AuthenticatorCredentials credentials) throws Exception {
    OAuthInfo oAuthInfo = Authenticator.getOAuthInfo(credentials);

    SslContextFactory sslContextFactory = new SslContextFactory();

    // Set up a Jetty HTTP client to use with CometD
    HttpClient httpClient = new HttpClient(sslContextFactory);
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT_MS);
    httpClient.start();

    // Use the Jackson implementation
    jsonContext = new JacksonJSONContextClient();

    Map<String, Object> transportOptions = new HashMap<>();
    transportOptions.put(ClientTransport.JSON_CONTEXT_OPTION, jsonContext);

    // Adds the OAuth header in LongPollingTransport
    LongPollingTransport transport = new LongPollingTransport(
      transportOptions, httpClient) {
      @Override
      protected void customize(Request exchange) {
        super.customize(exchange);
        exchange.header("Authorization", "OAuth " + oAuthInfo.getAccessToken());
      }
    };

    // Now set up the Bayeux client itself
    return new BayeuxClient(oAuthInfo.getInstanceURL() + DEFAULT_PUSH_ENDPOINT, transport);
  }

  public void createSalesforceListener() throws Exception {
    bayeuxClient = getClient(credentials);

    // Register Replay Extension with Bayeux Client
    bayeuxClient.addExtension(new ReplayExtension(this.dataMap));

    bayeuxClient.getChannel(Channel.META_HANDSHAKE).addListener
      ((ClientSessionChannel.MessageListener) (channel, message) -> {

        boolean success = message.isSuccessful();
        if (!success) {
          String error = (String) message.get("error");
          if (error != null) {
            throw new RuntimeException(String.format("Error in meta handshake, errorMessage: %s", error));
          } else if (message.get("exception") instanceof Exception) {
            Exception exception = (Exception) message.get("exception");
            if (exception != null) {
              throw new RuntimeException(String.format("Exception in meta handshake %s", exception));
            }
          } else {
            throw new RuntimeException(String.format("Error in meta handshake, message: %s", message));
          }

        }
      });

    bayeuxClient.getChannel(Channel.META_CONNECT).addListener(
      (ClientSessionChannel.MessageListener) (channel, message) -> {

        boolean success = message.isSuccessful();
        if (!success) {
          String error = (String) message.get("error");
          Map<String, Object> advice = message.getAdvice();

          if (error != null) {
            LOG.error("Error during CONNECT: {}", error);
            LOG.debug("Advice during CONNECT: {}", advice);
          }
          // Error Codes Reference in Salesforce Streaming :
          // https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/streaming_error_codes
          // .htm
          if (advice != null && advice.get("reconnect").equals("handshake")) {
            LOG.debug("Reconnecting to Salesforce Push Topic");
            try {
              reconnectToTopic();
            } catch (Exception e) {
              throw new RuntimeException("Error in reconnecting to salesforce ", e);
            }
          } else {
            throw new RuntimeException(String.format("Error in meta connect, errorMessage: %s Advice: %s", error,
                                                     advice));
          }
        }
      });

    bayeuxClient.getChannel(Channel.META_SUBSCRIBE).addListener(
      (ClientSessionChannel.MessageListener) (channel, message) -> {

        boolean success = message.isSuccessful();
        if (!success) {
          String error = (String) message.get("error");
          if (error != null) {
            throw new RuntimeException(String.format("Error in meta subscribe, errorMessage: %s", error));
          } else {
            throw new RuntimeException(String.format("Error in meta subscribe, message: %s", message));
          }
        }
      });

  }

  public void reconnectToTopic() throws Exception {
    disconnectStream();
    createSalesforceListener();
    waitForHandshake();
    subscribe();
  }

  private void waitForHandshake() {
    bayeuxClient.handshake();

    try {
      Awaitility.await()
        .atMost(SalesforcePushTopicListener.HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .pollInterval(SalesforcePushTopicListener.HANDSHAKE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(() -> bayeuxClient.isHandshook());
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException("Client could not handshake with Salesforce server", e);
    }
    LOG.debug("Client handshake done");
  }

  private void subscribe() {
    bayeuxClient.getChannel("/topic/" + topic).subscribe((channel, message) -> {
      LOG.debug("Message : {}", message);
      messagesQueue.add(jsonContext.getGenerator().generate(message.getDataAsMap()));
    });
  }

  public void disconnectStream() {
    bayeuxClient.getChannel("/topic/" + topic).unsubscribe();
    bayeuxClient.disconnect();
  }
}
