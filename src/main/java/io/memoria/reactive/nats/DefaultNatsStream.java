package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.nats.NatsConfig.TopicConfig;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamConfiguration.Builder;
import io.nats.client.api.StreamInfo;
import io.vavr.collection.Set;
import io.vavr.control.Try;
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

  DefaultNatsStream(NatsConfig config) throws IOException, InterruptedException, JetStreamApiException {
    this.config = config;
    ErrorListener e = new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
    var opt = new Options.Builder().server(config.url()).errorListener(e).build();
    this.nc = Nats.connect(opt);
    this.js = nc.jetStream();
    var streamInfo = createStreams(config.topics());
    log.info(streamInfo.toString());
  }

  private Set<StreamInfo> createStreams(Set<TopicConfig> streams) {
    var streamConfigs = StreamConfiguration.builder()
                                           .storageType(config.streamStorage())
                                           .denyDelete(true)
                                           .denyPurge(true);
    return streams.map(c -> streamConfigs.name(c.name()).subjects(c.partitionNames()))
                  .map(Builder::build)
                  .map(config -> Try.of(() -> NatsUtils.createOrUpdateStream(nc, config)))
                  .map(Try::get);
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    return msgs.flatMap(this::publish);
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    return Mono.fromCallable(() -> NatsUtils.subjectSize(nc, topic, partition));
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    return Mono.fromCallable(() -> createSubscription(topic, partition, offset))
               .flatMapMany(this::fetch)
               .map(m -> NatsUtils.toMsg(topic, partition, m));
  }

  private Mono<Msg> publish(Msg msg) {
    return Mono.fromCallable(() -> {
      var message = NatsUtils.toMessage(msg);
      var opts = PublishOptions.builder().clearExpected().messageId(msg.id().value()).build();
      return js.publishAsync(message, opts);
    }).flatMap(Mono::fromFuture).thenReturn(msg);
  }

  private JetStreamSubscription createSubscription(String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    return NatsUtils.pushSubscription(js, topic, partition, offset + 1);
  }

  private Flux<Message> fetch(JetStreamSubscription sub) {
    return Flux.generate((SynchronousSink<Message> sink) -> fetchOnce(sub, sink)).repeat();
  }

  private void fetchOnce(JetStreamSubscription sub, SynchronousSink<Message> sink) {
    try {
      nc.flushBuffer();
      var msg = sub.nextMessage(Duration.ofMillis(config.fetchWaitMillis()));
      if (msg != null) {
        sink.next(msg);
        msg.ack();
      }
      sink.complete();
    } catch (IOException | InterruptedException e) {
      sink.error(e);
    }
  }
}
