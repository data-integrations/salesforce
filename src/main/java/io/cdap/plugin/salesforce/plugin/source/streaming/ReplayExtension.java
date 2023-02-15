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

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSession;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * The class Replay extension.
 */
public class ReplayExtension implements ClientSession.Extension {
  private static final String EXTENSION_NAME = "replay";
  private static final String EVENT_KEY = "event";
  private static final String REPLAY_ID_KEY = "replayId";

  private final ConcurrentMap<String, Integer> dataMap;
  private final AtomicBoolean supported = new AtomicBoolean();

  public ReplayExtension(ConcurrentMap<String, Integer> dataMap) {
    this.dataMap = dataMap;
  }

  public static Integer getReplayId(Message.Mutable message) {
    Map<String, Object> data = message.getDataAsMap();
    @SuppressWarnings("unchecked")
    Optional<Integer> optional = resolve(() -> (Integer) ((Map<String, Object>) data.get(EVENT_KEY))
      .get(REPLAY_ID_KEY));
    return optional.orElse(null);
  }

  private static <T> Optional<T> resolve(Supplier<T> resolver) {
    try {
      T result = resolver.get();
      return Optional.ofNullable(result);
    } catch (NullPointerException e) {
      return Optional.empty();
    }
  }

  private static String topicWithoutQueryString(String fullTopic) {
    return fullTopic.split("\\?")[0];
  }

  @Override
  public boolean rcv(ClientSession session, Message.Mutable message) {
    Integer replayId = getReplayId(message);
    if (this.supported.get() && replayId != null) {
      try {
        String channel = topicWithoutQueryString(message.getChannel());
        dataMap.put(channel, replayId);
      } catch (ClassCastException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean rcvMeta(ClientSession session, Message.Mutable message) {
    switch (message.getChannel()) {
      case Channel.META_HANDSHAKE:
        Map<String, Object> ext = message.getExt(false);
        this.supported.set(ext != null && Boolean.TRUE.equals(ext.get(EXTENSION_NAME)));
    }
    return true;
  }

  @Override
  public boolean sendMeta(ClientSession session, Message.Mutable message) {
    switch (message.getChannel()) {
      case Channel.META_HANDSHAKE:
        message.getExt(true).put(EXTENSION_NAME, Boolean.TRUE);
        break;
      case Channel.META_SUBSCRIBE:
        if (supported.get()) {
          message.getExt(true).put(EXTENSION_NAME, dataMap);
        }
        break;
    }
    return true;
  }
}
