package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;

public class TestUtils {
  private TestUtils() {}

  public static TPConfig streamConfig(String topic, int partitions) {
    var tp = TP.create(topic, partitions);
    return new TPConfig(tp, StorageType.File, 1, 1000, 100, true, true);
  }
}
