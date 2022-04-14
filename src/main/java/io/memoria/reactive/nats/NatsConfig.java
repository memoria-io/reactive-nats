package io.memoria.reactive.nats;

import io.nats.client.api.StorageType;
import io.vavr.collection.Set;

public record NatsConfig(String url,
                         String streamName,
                         StorageType streamStorage,
                         Set<String> subjects,
                         int streamReplication,
                         long fetchWaitMillis,
                         int fetchBatchSize) {

}
