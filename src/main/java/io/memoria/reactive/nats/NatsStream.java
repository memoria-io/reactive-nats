package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Stream;
import io.nats.client.JetStreamApiException;

import java.io.IOException;

public interface NatsStream extends Stream {
  String TOPIC_PARTITION_SPLIT_TOKEN = "_";
  String MSG_ID_HEADER = "MSG_ID_HEADER";

  static Stream create(NatsConfig config) throws IOException, InterruptedException, JetStreamApiException {
    return new DefaultNatsStream(config);
  }
}
