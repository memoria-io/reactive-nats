package io.memoria.mnats;

import static io.memoria.mnats.NatsStream.NATS_MIN_DELAY;

public record MNatsConfig(String url, String streamName, long pullMaxWait, long pullEveryMillis, int fetchBatchSize) {
  public MNatsConfig {
    if (pullEveryMillis < pullMaxWait + NATS_MIN_DELAY) {
      var msg = "pullEveryMillis %d should be bigger than pullMaxWait(%d)+Nats minimum req delay(%d)";
      throw new IllegalArgumentException(msg.formatted(pullEveryMillis, pullMaxWait, NATS_MIN_DELAY));
    }
  }
}
