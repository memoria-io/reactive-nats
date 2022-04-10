package io.memoria.mnats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

@SuppressWarnings("ClassCanBeRecord")
class DefaultNatsStream implements NatsStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsStream.class.getName());
  private final MNatsConfig config;
  private final Connection nc;
  private final JetStream js;

  DefaultNatsStream(MNatsConfig config) throws IOException, InterruptedException {
    this.config = config;
    this.nc = Nats.connect(config.url());
    this.js = nc.jetStream();
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    //    var records = msgs.map(this::toRecord);
    //    return createSender().send(records).map(SenderResult::correlationMetadata);
    return null;
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    //    return Mono.fromCallable(() -> MNatsUtils.topicSize(topic, partition, consumerConfig));
    return null;
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    Mono.fromCallable(() -> pullSubscription(topic, partition, offset)).flatMapMany(this::pull);
    return null;
  }

  private JetStreamSubscription pullSubscription(String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    var cc = ConsumerConfiguration.builder().startSequence(offset).build();
    var pullOptions = PullSubscribeOptions.builder().durable("durable-name-is-required").configuration(cc).build();
    var subject = MNatsUtils.toSubject(topic, partition);
    return js.subscribe(subject, pullOptions);
  }

  private Flux<Message> pull(JetStreamSubscription sub) {
    var delay = Duration.ofMillis(config.pullEveryMillis());
    return Flux.generate((SynchronousSink<List<Message>> sink) -> {
      var msgs = sub.fetch(config.fetchBatchSize(), config.pullMaxWait());
      sink.next(msgs);
    }).delayElements(delay).flatMap(Flux::fromIterable);
  }
}
