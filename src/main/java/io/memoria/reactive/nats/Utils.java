package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.nats.Config.TopicConfig;
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
  static final String TOPIC_PARTITION_SPLIT_TOKEN = "_";
  static final String MSG_ID_HEADER = "MSG_ID_HEADER";
  static final String SUBJECT_EXT = ".stream";

  private Utils() {}

  static Try<StreamInfo> createOrUpdateStream(Connection nc, StreamConfiguration streamConfiguration) {
    return Try.of(() -> {
      var streamNames = nc.jetStreamManagement().getStreamNames();
      if (streamNames.contains(streamConfiguration.getName()))
        return nc.jetStreamManagement().updateStream(streamConfiguration);
      else
        return nc.jetStreamManagement().addStream(streamConfiguration);
    });
  }

  static JetStreamSubscription createSubscription(JetStream js, String streamName, long offset)
          throws IOException, JetStreamApiException {
    return pushSubscription(js, streamName, offset + 1);
  }

  static ErrorListener errorListener() {
    return new ErrorListener() {
      @Override
      public void errorOccurred(Connection conn, String error) {
        ErrorListener.super.errorOccurred(conn, error);
      }
    };
  }

  static void fetchOnce(Connection nc, JetStreamSubscription sub, SynchronousSink<Message> sink, long fetchWaitMillis) {
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

  static CompletableFuture<PublishAck> publishMsg(JetStream js, Msg msg) {
    var message = toMessage(msg);
    var opts = PublishOptions.builder().clearExpected().messageId(msg.id().value()).build();
    return js.publishAsync(message, opts);
  }

  static JetStreamSubscription pushSubscription(JetStream js, String streamName, long offset)
          throws IOException, JetStreamApiException {
    var cc = ConsumerConfiguration.builder()
                                  .ackPolicy(AckPolicy.None)
                                  .startSequence(offset)
                                  .replayPolicy(ReplayPolicy.Instant)
                                  .deliverPolicy(DeliverPolicy.ByStartSequence)
                                  .build();
    var pushOptions = PushSubscribeOptions.builder().ordered(true).stream(streamName).configuration(cc).build();
    var subjectName = toSubjectName(streamName);
    return js.subscribe(subjectName, pushOptions);
  }

  static long size(Connection nc, String streamName) throws IOException, JetStreamApiException {
    var subjectName = toSubjectName(streamName);
    return streamInfo(nc, streamName).map(StreamInfo::getStreamState)
                                     .map(StreamState::getSubjects)
                                     .flatMap(Option::of)
                                     .map(List::ofAll)
                                     .getOrElse(List::empty)
                                     .find(s -> s.getName().equalsIgnoreCase(subjectName))
                                     .map(Subject::getCount)
                                     .getOrElse(0L);
  }

  static Option<StreamInfo> streamInfo(Connection nc, String streamName) throws IOException, JetStreamApiException {
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

  static Message toMessage(Msg msg) {
    var streamName = toStreamName(msg.topic(), msg.partition());
    var subject = toSubjectName(streamName);
    var headers = new Headers();
    headers.add(MSG_ID_HEADER, msg.id().value());
    var data = msg.value().getBytes(StandardCharsets.UTF_8);
    return NatsMessage.builder().subject(streamName).subject(subject).headers(headers).data(data).build();
  }

  static Msg toMsg(String topic, int partition, Message message) {
    var id = Id.of(message.getHeaders().getFirst(MSG_ID_HEADER));
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(topic, partition, id, value);
  }

  static Options toOptions(Config config) {
    return new Options.Builder().server(config.url()).errorListener(errorListener()).build();
  }

  static StreamConfiguration toStreamConfiguration(TopicConfig c) {
    var streamName = toStreamName(c.name(), c.partition());
    var subjectName = toSubjectName(streamName);
    return StreamConfiguration.builder()
                              .storageType(c.storageType())
                              .denyDelete(c.denyDelete())
                              .denyPurge(c.denyPurge())
                              .name(streamName)
                              .subjects(subjectName)
                              .build();
  }

  static String toStreamName(String topic, int partition) {
    return "%s%s%d".formatted(topic, TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }

  static String toSubjectName(String streamName) {
    return streamName + SUBJECT_EXT;
  }
}
