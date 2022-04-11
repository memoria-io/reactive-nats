package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;
import io.vavr.collection.Set;

import static io.memoria.reactive.nats.NatsStream.NATS_MIN_DELAY;

public record NatsConfig(String url,
                         String streamName,
                         StorageType streamStorage,
                         Set<String> subjects,
                         int streamReplication,
                         long pullMaxWait,
                         long pullEveryMillis,
                         int fetchBatchSize) {
  public NatsConfig {
    if (pullEveryMillis < pullMaxWait + NATS_MIN_DELAY) {
      var msg = "pullEveryMillis %d should be bigger than pullMaxWait(%d)+Nats minimum req delay(%d)";
      throw new IllegalArgumentException(msg.formatted(pullEveryMillis, pullMaxWait, NATS_MIN_DELAY));
    }
  }
}
