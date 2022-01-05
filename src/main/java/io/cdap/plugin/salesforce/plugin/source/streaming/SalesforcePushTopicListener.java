/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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
  private static final int CONNECTION_TIMEOUT_SECONDS = 110000;
  private static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);

  private static final int HANDSHAKE_CHECK_INTERVAL_MS = 1000;

  // store message string not JSONObject, since it's not serializable for later Spark usage
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>();

  private final AuthenticatorCredentials credentials;
  private final String topic;

  private JSONContext.Client jsonContext;

  public SalesforcePushTopicListener(AuthenticatorCredentials credentials, String topic) {
    this.credentials = credentials;
    this.topic = topic;
  }

  /**
   * Start the Bayeux Client which listens to the Salesforce PushTopic and saves received messages
   * to the queue.
   */
  public void start() {
    try {
      BayeuxClient bayeuxClient = getClient(credentials);
      waitForHandshake(bayeuxClient, HANDSHAKE_TIMEOUT_MS, HANDSHAKE_CHECK_INTERVAL_MS);
      LOG.debug("Client handshake done");
      bayeuxClient.getChannel("/topic/" + topic).subscribe((channel, message) -> {
        messagesQueue.add(jsonContext.getGenerator().generate(message.getDataAsMap()));
      });

    } catch (Exception e) {
      throw new RuntimeException("Could not start client", e);
    }
  }

  /**
   * Retrieves message from the messages queue, waiting up to the
   * specified wait time if necessary for an element to become available.
   *
   * @param timeout how long to wait before giving up
   * @param unit timeunit of timeout
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
    httpClient.setConnectTimeout(CONNECTION_TIMEOUT_SECONDS);
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
    BayeuxClient client = new BayeuxClient(oAuthInfo.getInstanceURL() + DEFAULT_PUSH_ENDPOINT, transport);
    client.handshake();

    return client;
  }

  private void waitForHandshake(BayeuxClient client,
                                long timeoutInMilliseconds, long intervalInMilliseconds) {
    try {
      Awaitility.await()
        .atMost(timeoutInMilliseconds, TimeUnit.MILLISECONDS)
        .pollInterval(intervalInMilliseconds, TimeUnit.MILLISECONDS)
        .until(() -> client.isHandshook());
    } catch (ConditionTimeoutException e) {
      throw new IllegalStateException("Client could not handshake with Salesforce server", e);
    }
  }
}
