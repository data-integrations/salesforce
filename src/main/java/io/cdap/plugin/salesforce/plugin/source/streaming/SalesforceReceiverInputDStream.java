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

import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.reflect.ClassTag;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SalesforceReceiverInputDStream
 * @param <T>
 */
public class SalesforceReceiverInputDStream<T> extends ReceiverInputDStream<T>  {

  private static final Logger LOG = LoggerFactory.getLogger(SalesforceReceiverInputDStream.class);

  private final Receiver<T> receiver;
  private AtomicBoolean receiverFailed;

  public SalesforceReceiverInputDStream(StreamingContext sc, ClassTag tag, Receiver<T> receiver) {
    super(sc, tag);
    this.receiver = receiver;
    this.receiverFailed = new AtomicBoolean(false);
  }

  @Override
  public Receiver<T> getReceiver() {
    return receiver;
  }

  @Override
  public Option<RDD<T>> compute(Time validTime) {
    //LOG.info("Compute ")
    if (receiverFailed.get()) {
      throw new RuntimeException("Receiver has reported an exception. Please check logs.");
    }
    return super.compute(validTime);
  }

  public void setReceiverFailed() {
    receiverFailed.set(true);
  }
}
