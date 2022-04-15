package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;
import io.vavr.collection.List;
import io.vavr.collection.Set;

public record NatsConfig(String url,
                         Set<TopicConfig> topics,
                         StorageType streamStorage,
                         int streamReplication,
                         long fetchWaitMillis,
                         int fetchBatchSize) {

  public record TopicConfig(String name, int partitions) {
    public TopicConfig {
      if (name == null || name.isEmpty()) {
        throw new IllegalArgumentException("Name can't be null or empty string");
      }
      if (partitions < 1) {
        throw new IllegalArgumentException("Number of partitions can't be less than 1");
      }
    }

    public java.util.List<String> partitionNames() {
      return List.range(0, partitions)
                 .map(i -> "%s%s%d".formatted(name, NatsStream.TOPIC_PARTITION_SPLIT_TOKEN, i))
                 .toJavaList();
    }
  }
}
