package io.memoria.reactive.nats;

import io.memoria.reactive.nats.Config.TopicConfig;
import io.nats.client.api.StorageType;

public class TestUtils {
  private TestUtils() {}

  public static TopicConfig streamConfig(String topic, int partitions) {
    return new TopicConfig(topic, partitions, StorageType.File, 1, 1000, 100, true, true);
  }
}
