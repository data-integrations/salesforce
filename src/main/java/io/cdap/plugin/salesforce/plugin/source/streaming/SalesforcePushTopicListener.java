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

import com.google.common.base.Strings;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.authenticator.Authenticator;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
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

import java.io.IOException;
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
  private static final long CONNECTION_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);
  private static final long HANDSHAKE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(110);

  private static final int HANDSHAKE_CHECK_INTERVAL_MS = 1000;

  // store message string not JSONObject, since it's not serializable for later Spark usage
  private final BlockingQueue<String> messagesQueue = new LinkedBlockingQueue<>();

  private final AuthenticatorCredentials credentials;
  private final String topic;
  private final long maxRetryTimeInMillis;
  private final SalesforceReceiver receiver;
  private BayeuxClient bayeuxClient;
  private JSONContext.Client jsonContext;
  private long firstErrorOccurrenceTime = 0L;

  public SalesforcePushTopicListener(SalesforceReceiver receiver) {
    this.credentials = receiver.getCredentials();
    this.topic = receiver.getTopic();
    this.receiver = receiver;
    this.maxRetryTimeInMillis = getMaxRetryTimeInMills(receiver);
  }

  /**
   * This method wil return maxRetryTimeInMins from runtime arguments and
   * if not provided by user then return default value
   *
   * @param receiver
   * @return maxRetryTimeInMins
   */
  private long getMaxRetryTimeInMills(SalesforceReceiver receiver) {
    String maxRetryTimeInMins = receiver.getArguments().get(SalesforceConstants.PROPERTY_MAX_RETRY_TIME_IN_MINS);
    if (maxRetryTimeInMins != null) {
      try {
        return TimeUnit.MILLISECONDS.convert(Long.parseLong(maxRetryTimeInMins), TimeUnit.MINUTES);
      } catch (NumberFormatException nfe) {
        // do nothing, this method will return default value below.
      }
    }
    return TimeUnit.MILLISECONDS.convert(SalesforceConstants.DEFAULT_MAX_RETRY_TIME_IN_MINS, TimeUnit.MINUTES);
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
    if (!Strings.isNullOrEmpty(credentials.getProxyUrl())) {
      Authenticator.setProxy(credentials, httpClient);
    }

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
    bayeuxClient.getChannel(Channel.META_HANDSHAKE).addListener
      ((ClientSessionChannel.MessageListener) (channel, message) -> {
        boolean success = message.isSuccessful();
        if (!success) {
          stopReceiver(message, Channel.META_HANDSHAKE);
        }
      });
    bayeuxClient.getChannel(Channel.META_CONNECT).addListener(
      (ClientSessionChannel.MessageListener) (channel, message) -> {

        boolean success = message.isSuccessful();
        if (!success) {
          LOG.debug(String.format("Error in meta connect, message: %s", message));
          String error = (String) message.get("error");
          Map<String, Object> advice = message.getAdvice();

          if (error != null) {
            LOG.error("Error during CONNECT: {}", error);
            LOG.debug("Advice during CONNECT: {}", advice);
          }
          // Error Codes Reference in Salesforce Streaming :
          // https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/streaming_error_codes
          // .htm
          if (advice != null && "handshake".equals(advice.get("reconnect"))) {
            LOG.info("Reconnecting to Salesforce Push Topic");
            try {
              reconnectToTopic();
            } catch (Exception e) {
              stopReceiver(message, Channel.META_CONNECT);
            }
          } else {
            stopReceiver(message, Channel.META_CONNECT);
          }
        }
      });

    bayeuxClient.getChannel(Channel.META_SUBSCRIBE).addListener(
      (ClientSessionChannel.MessageListener) (channel, message) -> {

        boolean success = message.isSuccessful();
        if (!success) {
          stopReceiver(message, Channel.META_SUBSCRIBE);
        }
      });
  }

  public void reconnectToTopic() throws Exception {
    disconnectStream();
    createSalesforceListener();
    waitForHandshake();
    subscribe();
  }

  private void waitForHandshake() throws IOException {
    bayeuxClient.handshake();

    try {
      Awaitility.await()
        .atMost(SalesforcePushTopicListener.HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
        .pollInterval(SalesforcePushTopicListener.HANDSHAKE_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS)
        .until(() -> bayeuxClient.isHandshook());
    } catch (ConditionTimeoutException e) {
      throw new IOException("Client could not handshake with Salesforce server", e);
    }
    LOG.debug("Client handshake done");
  }

  private void subscribe() {
    bayeuxClient.getChannel("/topic/" + topic).subscribe((channel, message) ->
                                                           messagesQueue.add(jsonContext.getGenerator()
                                                                               .generate(message.getDataAsMap())));
  }

  public void disconnectStream() {
    if (bayeuxClient != null) {
      bayeuxClient.getChannel("/topic/" + topic).unsubscribe();
      bayeuxClient.disconnect();
    }
  }

  /**
   * @param message message information got from the salesforce side
   * @param channel Channel for which this information is passed
   */
  private void stopReceiver(Message message, String channel) {
    if (firstErrorOccurrenceTime == 0L) {
      firstErrorOccurrenceTime = System.currentTimeMillis();
      LOG.debug("First error request came at : {} with message : {}", firstErrorOccurrenceTime, message);
      return;
    } else if (!(System.currentTimeMillis() - firstErrorOccurrenceTime >= this.maxRetryTimeInMillis)) {
      return;
    }
    String error = (String) message.get("error");
    if (error != null) {
      receiver.stop("Error in reconnecting to salesforce",
                    new RuntimeException(String.format("Error in %s, errorMessage:%s Advice: %s", channel,
                                                       error, message.getAdvice())));
    } else {
      receiver.stop("Error in reconnecting to salesforce",
                    new RuntimeException(String.format("Error in %s, message: %s", channel,
                                                       message)));
    }
  }

}
