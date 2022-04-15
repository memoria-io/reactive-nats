package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;
import io.vavr.collection.List;
import io.vavr.collection.Set;

public record Config(String url, Set<StreamConfig> streams) {
  static final long DEFAULT_FETCH_WAIT = 1000L;

  public record StreamConfig(String name,
                             int partitions,
                             StorageType storageType,
                             int streamReplication,
                             long fetchWaitMillis,
                             int fetchBatchSize,
                             boolean denyDelete,
                             boolean denyPurge) {
    public StreamConfig {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name can't be null or empty string");
      }
      if (partitions < 1) {
        throw new IllegalArgumentException("Number of partitions can't be less than 1");
      }
    }

    public java.util.List<String> streamPartitions() {
      return List.range(0, partitions).map(i -> Utils.topicPartition(name, i)).toJavaList();
    }
  }
}
