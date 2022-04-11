package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Stream;

import java.io.IOException;

public interface NatsStream extends Stream {
  String TOPIC_PARTITION_SPLIT_TOKEN = "_";
  int NATS_MIN_DELAY = 20;

  static Stream create(NatsConfig config) throws IOException, InterruptedException {
    return new DefaultNatsStream(config);
  }
}
