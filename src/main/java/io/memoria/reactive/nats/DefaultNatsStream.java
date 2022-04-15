package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.nats.Config.TopicConfig;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.api.StreamInfo;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;

import static io.memoria.reactive.nats.Config.DEFAULT_FETCH_WAIT;
import static io.memoria.reactive.nats.Utils.createOrUpdateStream;
import static io.memoria.reactive.nats.Utils.createSubscription;
import static io.memoria.reactive.nats.Utils.toMsg;
import static io.memoria.reactive.nats.Utils.toStreamName;

class DefaultNatsStream implements NatsStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsStream.class.getName());
  private final Config config;
  private final Connection nc;
  private final JetStream js;

  DefaultNatsStream(Config config) throws IOException, InterruptedException {
    this.config = config;
    this.nc = Nats.connect(Utils.toOptions(config));
    this.js = nc.jetStream();
    config.streams()
          .map(Utils::toStreamConfiguration)
          .map(c -> createOrUpdateStream(nc, c))
          .map(Try::get)
          .map(StreamInfo::toString)
          .forEach(log::info);
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    return msgs.flatMap(this::publish);
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    var streamName = Utils.toStreamName(topic, partition);
    return Mono.fromCallable(() -> Utils.size(nc, streamName));
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    var streamName = toStreamName(topic, partition);
    return Mono.fromCallable(() -> createSubscription(js, streamName, offset))
               .flatMapMany(sub -> this.fetch(sub, topic))
               .map(m -> toMsg(topic, partition, m));
  }

  private Flux<Message> fetch(JetStreamSubscription sub, String topic) {
    var wait = config.streams()
                     .find(s -> s.name().equalsIgnoreCase(topic))
                     .map(TopicConfig::fetchWaitMillis)
                     .getOrElse(DEFAULT_FETCH_WAIT);
    return Flux.generate((SynchronousSink<Message> sink) -> Utils.fetchOnce(nc, sub, sink, wait)).repeat();
  }

  private Mono<Msg> publish(Msg msg) {
    return Mono.fromCallable(() -> Utils.publishMsg(js, msg)).flatMap(Mono::fromFuture).thenReturn(msg);
  }
}
