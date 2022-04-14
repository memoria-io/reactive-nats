package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StreamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.FluxSink.OverflowStrategy;
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
    var subject = NatsUtils.toSubject(topic, partition);
    return Mono.fromCallable(() -> NatsUtils.pushSubscription(js, config.streamName(), subject, offset + 1))
               .flatMapMany(this::fetchMsg)
               .map(m -> NatsUtils.toMsg(topic, partition, m));
    //    return sub(config.streamName(), subject, offset + 1).map(m -> NatsUtils.toMsg(topic, partition, m));
  }

  private Mono<Msg> publish(Msg msg) {
    return Mono.fromCallable(() -> {
      var message = NatsUtils.toMessage(msg);
      var opts = PublishOptions.builder().clearExpected().messageId(msg.id().value()).build();
      return js.publishAsync(message, opts);
    }).flatMap(Mono::fromFuture).thenReturn(msg);
  }

  private Flux<Message> fetchMsg(JetStreamSubscription sub) {
    return Flux.generate((SynchronousSink<Message> sink) -> {
      try {
        nc.flushBuffer();
        var msg = sub.nextMessage(Duration.ofMillis(config.fetchWaitMillis()));
        // TODO handle completion without error
        if (msg != null) {
          sink.next(msg);
          msg.ack();
        }
        sink.complete();
      } catch (IOException | InterruptedException e) {
        sink.error(e);
      }
    }).repeat();
  }

  public Flux<Message> sub(String stream, String subject, long offset) {
    var disp = nc.createDispatcher();
    var cc = ConsumerConfiguration.builder()
                                  .ackPolicy(AckPolicy.None)
                                  .startSequence(offset)
                                  .replayPolicy(ReplayPolicy.Instant)
                                  .deliverPolicy(DeliverPolicy.ByStartSequence)
                                  .build();
    var pushOptions = PushSubscribeOptions.builder().ordered(true).stream(stream).configuration(cc).build();
    boolean autoAck = true;
    var f = Flux.push((FluxSink<Message> sink) -> {
      MessageHandler handler = (msg) -> {
        sink.next(msg);
        msg.ack();
      };
      try {
        var subscription = js.subscribe(subject, null, disp, handler, autoAck, pushOptions);
      } catch (IOException | JetStreamApiException e) {
        sink.error(e);
      }
    }, OverflowStrategy.ERROR);
    return f;
  }
}
