package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.time.Duration;

class DefaultNatsStream implements NatsStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsStream.class.getName());
  private final NatsConfig config;
  private final Connection nc;
  private final JetStream js;
  private final StreamConfiguration.Builder streamConfigs;

  DefaultNatsStream(NatsConfig config) throws IOException, InterruptedException, JetStreamApiException {
    this.config = config;
    this.nc = Nats.connect(config.url());
    this.js = nc.jetStream();
    this.streamConfigs = StreamConfiguration.builder()
                                            .name(config.streamName())
                                            .storageType(config.streamStorage())
                                            .subjects(config.subjects().toJavaSet())
                                            .denyDelete(true)
                                            .denyPurge(true);
    var streamInfo = NatsUtils.createOrUpdateStream(nc, this.streamConfigs.build());
    log.info(streamInfo.toString());
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    return msgs.flatMap(this::publish);
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    var subject = NatsUtils.toSubject(topic, partition);
    return Mono.fromCallable(() -> NatsUtils.subjectSize(nc, subject));
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    return Mono.fromCallable(() -> NatsUtils.pullSubscription(js, topic, partition, offset))
               .flatMapMany(this::pull)
               .map(NatsUtils::toMsg);
  }

  private Mono<Msg> publish(Msg msg) {
    var pubOpt = PublishOptions.builder().messageId(msg.id().value()).build();
    var message = NatsUtils.toNatsMsg(msg);
    return Mono.fromFuture(() -> NatsUtils.publish(js, message, pubOpt)).thenReturn(msg);
  }

  private Flux<Message> pull(JetStreamSubscription sub) {
    var delay = Duration.ofMillis(config.pullEveryMillis());
    return Flux.generate((SynchronousSink<java.util.List<Message>> sink) -> {
      var msgs = sub.fetch(config.fetchBatchSize(), config.pullMaxWait());
      sink.next(msgs);
    }).delayElements(delay).flatMap(Flux::fromIterable);
  }
}
