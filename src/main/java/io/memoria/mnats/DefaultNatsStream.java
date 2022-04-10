package io.memoria.mnats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.Subject;
import io.vavr.collection.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.time.Duration;

@SuppressWarnings("ClassCanBeRecord")
class DefaultNatsStream implements NatsStream {
  private static final Logger log = LoggerFactory.getLogger(DefaultNatsStream.class.getName());
  private final NatsConfig config;
  private final Connection nc;
  private final JetStream js;

  DefaultNatsStream(NatsConfig config) throws IOException, InterruptedException {
    this.config = config;
    this.nc = Nats.connect(config.url());
    this.js = nc.jetStream();
  }

  @Override
  public Flux<Msg> publish(Flux<Msg> msgs) {
    return msgs.flatMap(this::publish);
  }

  @Override
  public Mono<Long> size(String topic, int partition) {
    var subject = NatsUtils.toSubject(topic, partition);
    return Mono.fromCallable(() -> subjectCount(subject));
  }

  @Override
  public Flux<Msg> subscribe(String topic, int partition, long offset) {
    return Mono.fromCallable(() -> pullSubscription(topic, partition, offset))
               .flatMapMany(this::pull)
               .map(NatsUtils::toMsg);
  }

  private Mono<Msg> publish(Msg msg) {
    var pubOpt = PublishOptions.builder().messageId(msg.id().value()).build();
    var message = NatsUtils.toNatsMsg(msg);
    return Mono.fromFuture(() -> js.publishAsync(message, pubOpt)).thenReturn(msg);
  }

  private Flux<Message> pull(JetStreamSubscription sub) {
    var delay = Duration.ofMillis(config.pullEveryMillis());
    return Flux.generate((SynchronousSink<java.util.List<Message>> sink) -> {
      var msgs = sub.fetch(config.fetchBatchSize(), config.pullMaxWait());
      sink.next(msgs);
    }).delayElements(delay).flatMap(Flux::fromIterable);
  }

  private JetStreamSubscription pullSubscription(String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    var subject = NatsUtils.toSubject(topic, partition);
    var consumerName = "%s_consumer".formatted(subject);
    var cc = ConsumerConfiguration.builder().durable(consumerName).startSequence(offset).build();
    var pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
    return js.subscribe(subject, pullOptions);
  }

  private long subjectCount(String subjectName) throws IOException, JetStreamApiException {
    var subjects = List.ofAll(nc.jetStreamManagement()
                                .getStreamInfo(config.streamName())
                                .getStreamState()
                                .getSubjects());
    return subjects.find(s -> s.getName().equalsIgnoreCase(subjectName)).map(Subject::getCount).getOrElse(0L);
  }
}
