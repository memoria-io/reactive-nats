package io.memoria.mnats;

import io.memoria.reactive.core.stream.Stream;

import java.io.IOException;

public interface NatsStream extends Stream {
  String TOPIC_PARTITION_SPLIT_TOKEN = ".";
  int NATS_MIN_DELAY = 20;

  static Stream create(MNatsConfig config) throws IOException, InterruptedException {
    return new DefaultNatsStream(config);
  }
}
