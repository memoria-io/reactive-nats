package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;

class DefaultNatsStream implements NatsStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsStream.class.getName());
  private final NatsConfig config;
  private final Connection nc;
  private final JetStream js;

  DefaultNatsStream(NatsConfig config) throws IOException, InterruptedException, JetStreamApiException {
    this.config = config;
    this.nc = Nats.connect(config.url());
    this.js = nc.jetStream();
    var streamConfigs = StreamConfiguration.builder()
                                           .name(config.streamName())
                                           .storageType(config.streamStorage())
                                           .subjects(config.subjects().toJavaSet())
                                           .denyDelete(true)
                                           .denyPurge(true);
    var streamInfo = NatsUtils.createOrUpdateStream(nc, streamConfigs.build());
    log.info(streamInfo.toString());
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    return msgs.flatMap(this::publish);
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    var subject = NatsUtils.toSubject(topic, partition);
    return Mono.fromCallable(() -> NatsUtils.subjectSize(nc, config.streamName(), subject));
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    return Mono.fromCallable(() -> NatsUtils.pullSubscription(js, topic, partition, offset))
               .flatMapMany(this::fetch)
               .map(m -> NatsUtils.toMsg(topic, partition, m));
  }

  private Mono<Msg> publish(Msg msg) {
    var message = NatsUtils.toMessage(msg);
    return Mono.fromFuture(js.publishAsync(message)).thenReturn(msg);
  }

  private Flux<Message> fetch(JetStreamSubscription sub) {
    return Flux.generate((SynchronousSink<java.util.List<Message>> sink) -> {
      var msgs = sub.fetch(config.fetchBatchSize(), config.pullMaxWait());
      sink.next(msgs);
    }).subscribeOn(Schedulers.boundedElastic()).flatMap(Flux::fromIterable);
  }
}
