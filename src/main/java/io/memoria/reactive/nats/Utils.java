package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.nats.Config.StreamConfig;
import io.nats.client.Connection;
import io.nats.client.ErrorListener;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.api.PublishAck;
import io.nats.client.api.ReplayPolicy;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamInfoOptions;
import io.nats.client.api.StreamState;
import io.nats.client.api.Subject;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.control.Option;
import io.vavr.control.Try;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

class Utils {
  public static final String TOPIC_PARTITION_SPLIT_TOKEN = "_";
  public static final String MSG_ID_HEADER = "MSG_ID_HEADER";

  private Utils() {}

  public static Try<StreamInfo> createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration) {
    return Try.of(() -> {
      var streamNames = nc.jetStreamManagement().getStreamNames();
      if (streamNames.contains(streamConfiguration.getName()))
        return nc.jetStreamManagement().updateStream(streamConfiguration);
      else
        return nc.jetStreamManagement().addStream(streamConfiguration);
    });
  }

  public static JetStreamSubscription createSubscription(JetStream js, String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    return pushSubscription(js, topic, partition, offset + 1);
  }

  public static void fetchOnce(Connection nc,
                               JetStreamSubscription sub,
                               SynchronousSink<Message> sink,
                               long fetchWaitMillis) {
    try {
      nc.flushBuffer();
      var msg = sub.nextMessage(Duration.ofMillis(fetchWaitMillis));
      if (msg != null) {
        sink.next(msg);
        msg.ack();
      }
      sink.complete();
    } catch (IOException | InterruptedException e) {
      sink.error(e);
    }
  }

  public static CompletableFuture<PublishAck> publishMsg(JetStream js, Msg msg) {
    var message = toMessage(msg);
    var opts = PublishOptions.builder().clearExpected().messageId(msg.id().value()).build();
    return js.publishAsync(message, opts);
  }

  public static JetStreamSubscription pushSubscription(JetStream js, String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    var subject = toSubject(topic, partition);
    var cc = ConsumerConfiguration.builder()
                                  .ackPolicy(AckPolicy.None)
                                  .startSequence(offset)
                                  .replayPolicy(ReplayPolicy.Instant)
                                  .deliverPolicy(DeliverPolicy.ByStartSequence)
                                  .build();
    var pushOptions = PushSubscribeOptions.builder().ordered(true).stream(topic).configuration(cc).build();
    return js.subscribe(subject, pushOptions);
  }

  public static Option<StreamInfo> streamInfo(Connection nc, String streamName)
          throws IOException, JetStreamApiException {
    try {
      var opts = StreamInfoOptions.allSubjects();
      return Option.some(nc.jetStreamManagement().getStreamInfo(streamName, opts));
    } catch (JetStreamApiException e) {
      if (e.getErrorCode() == 404) {
        return Option.none();
      } else {
        throw e;
      }
    }
  }

  public static long subjectSize(Connection nc, String topic, int partition) throws IOException, JetStreamApiException {
    var subject = Utils.toSubject(topic, partition);
    return streamInfo(nc, topic).map(StreamInfo::getStreamState)
                                .map(StreamState::getSubjects)
                                .flatMap(Option::of)
                                .map(List::ofAll)
                                .getOrElse(List::empty)
                                .find(s -> s.getName().equalsIgnoreCase(subject))
                                .map(Subject::getCount)
                                .getOrElse(0L);
  }

  public static Message toMessage(Msg msg) {
    var subject = toSubject(msg.topic(), msg.partition());
    var headers = new Headers();
    headers.add(MSG_ID_HEADER, msg.id().value());
    var data = msg.value().getBytes(StandardCharsets.UTF_8);
    return NatsMessage.builder().subject(subject).headers(headers).data(data).build();
  }

  public static Msg toMsg(String topic, int partition, Message message) {
    var id = Id.of(message.getHeaders().getFirst(MSG_ID_HEADER));
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(topic, partition, id, value);
  }

  public static Options toOptions(Config config) {
    return new Options.Builder().server(config.url()).errorListener(errorListener()).build();
  }

  public static StreamConfiguration toStreamConfiguration(StreamConfig c) {
    return StreamConfiguration.builder()
                              .storageType(c.storageType())
                              .denyDelete(c.denyDelete())
                              .denyPurge(c.denyPurge())
                              .name(c.name())
                              .subjects(c.streamPartitions())
                              .build();
  }

  public static String toSubject(String topic, int partition) {
    return "%s%s%d".formatted(topic, TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }

  public static String topicPartition(String topic, int partition) {
    return "%s%s%d".formatted(topic, TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }

  private static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }
}
