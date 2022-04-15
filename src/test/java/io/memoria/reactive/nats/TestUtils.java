package io.memoria.reactive.nats;

import io.memoria.reactive.nats.Config.StreamConfig;
import io.nats.client.api.StorageType;

public class TestUtils {
  private TestUtils() {}

  public static StreamConfig streamConfig(String topic, int partition) {
    return new StreamConfig(topic, 1, StorageType.File, 1, 1000, 100, true, true);
  }
}
