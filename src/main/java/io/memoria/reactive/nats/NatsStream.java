package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Stream;
import io.nats.client.JetStreamApiException;

import java.io.IOException;

public interface NatsStream extends Stream {
  static Stream create(Config config) throws IOException, InterruptedException, JetStreamApiException {
    return new DefaultNatsStream(config);
  }
}
