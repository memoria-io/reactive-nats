package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;
import io.vavr.collection.Set;

public record Config(String url, Set<TopicConfig> streams) {
  static final long DEFAULT_FETCH_WAIT = 1000L;

  public record TopicConfig(String name,
                            int partition,
                            StorageType storageType,
                            int streamReplication,
                            long fetchWaitMillis,
                            int fetchBatchSize,
                            boolean denyDelete,
                            boolean denyPurge) {
    public TopicConfig {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name can't be null or empty string");
      }
    }
  }
}
