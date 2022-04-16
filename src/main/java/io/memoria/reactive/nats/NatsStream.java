package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Stream;
import io.nats.client.JetStreamApiException;

import java.io.IOException;

public interface NatsStream extends Stream {
  String MESSAGE_ID_HEADER = "MSG_ID_HEADER";

  static Stream create(Config config) throws IOException, InterruptedException, JetStreamApiException {
    return new DefaultNatsStream(config);
  }
}
