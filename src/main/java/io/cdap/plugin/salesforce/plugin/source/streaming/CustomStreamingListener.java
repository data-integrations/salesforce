/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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

import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom progress Listener. This will be added to streaming context to monitor the progress of a Spark Streaming job.
 * We are using it specifically to intimate us when receiver got stopped,
 * so that we can stop the pipeline when receiver got stopped.
 */
public class CustomStreamingListener implements StreamingListener {

  private final AtomicBoolean receiverStopped = new AtomicBoolean(false);

  public CustomStreamingListener() {
  }

  public AtomicBoolean isReceiverStopped() {
    return receiverStopped;
  }

  @Override
  public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
    this.isReceiverStopped().set(true);
  }

  @Override
  public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
  }

  @Override
  public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
  }

  @Override
  public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
  }

  @Override
  public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
  }

  @Override
  public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
  }

  @Override
  public void onReceiverError(StreamingListenerReceiverError receiverError) {
  }

  @Override
  public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
  }
}
