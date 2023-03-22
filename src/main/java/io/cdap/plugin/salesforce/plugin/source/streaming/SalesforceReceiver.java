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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Spark receiver to receive Salesforce push topic events
 */
public class SalesforceReceiver extends Receiver<String> {
  private static final Logger LOG = LoggerFactory.getLogger(SalesforceReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "salesforce_streaming_api_listener";
  // every x seconds thread wakes up and checks if stream is not yet stopped
  private static final long GET_MESSAGE_TIMEOUT_SECONDS = 2;

  private final AuthenticatorCredentials credentials;
  private final String topic;
  private SalesforcePushTopicListener pushTopicListener;
  private final Arguments arguments;

  SalesforceReceiver(AuthenticatorCredentials credentials, String topic, Arguments arguments) {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.credentials = credentials;
    this.topic = topic;
    this.arguments = arguments;
  }

  @Override
  public void onStart() {
    pushTopicListener = new SalesforcePushTopicListener(this);
    pushTopicListener.start();

    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat(RECEIVER_THREAD_NAME + "-%d")
      .build();
    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    //Shutdown thread pool executor
  }

  private void receive() {
    try {
      while (!isStopped()) {
        String message = pushTopicListener.getMessage(GET_MESSAGE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (message != null) {
          store(message);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Exception while receiving messages from pushTopic";
      // Since it's top level method of thread, we need to log the exception or it will be unseen
      LOG.error(errorMessage, e);
      stop(errorMessage, e);
    }
  }

  public AuthenticatorCredentials getCredentials() {
    return credentials;
  }

  public String getTopic() {
    return topic;
  }

  public Arguments getArguments() {
    return arguments;
  }
}
