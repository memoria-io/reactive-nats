package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
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

  static JetStreamSubscription pushSubscription(JetStream js, TP TP, long offset)
          throws IOException, JetStreamApiException {
    var cc = ConsumerConfiguration.builder()
                                  .ackPolicy(AckPolicy.None)
                                  .startSequence(offset)
                                  .replayPolicy(ReplayPolicy.Instant)
                                  .deliverPolicy(DeliverPolicy.ByStartSequence)
                                  .build();
    var pushOptions = PushSubscribeOptions.builder().ordered(true).stream(TP.streamName()).configuration(cc).build();
    return js.subscribe(TP.subjectName(), pushOptions);
  }

  static long size(Connection nc, TP TP) throws IOException, JetStreamApiException {
    return streamInfo(nc, TP.streamName()).map(StreamInfo::getStreamState)
                                          .map(StreamState::getSubjects)
                                          .flatMap(Option::of)
                                          .map(List::ofAll)
                                          .getOrElse(List::empty)
                                          .find(s -> s.getName().equals(TP.subjectName()))
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
    var tp = TP.fromMsg(msg);
    var headers = new Headers();
    headers.add(NatsStream.MESSAGE_ID_HEADER, msg.id().value());
    return NatsMessage.builder().subject(tp.subjectName()).headers(headers).data(msg.value()).build();
  }

  static Msg toMsg(Message message) {
    var id = Id.of(message.getHeaders().getFirst(NatsStream.MESSAGE_ID_HEADER));
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    var tp = TP.fromSubject(message.getSubject());
    return new Msg(tp.topic(), tp.partition(), id, value);
  }

  static Options toOptions(Config config) {
    return new Options.Builder().server(config.url()).errorListener(errorListener()).build();
  }

  static StreamConfiguration toStreamConfiguration(TPConfig c) {
    return StreamConfiguration.builder()
                              .storageType(c.storageType())
                              .denyDelete(c.denyDelete())
                              .denyPurge(c.denyPurge())
                              .name(c.tp().streamName())
                              .subjects(c.tp().subjectName())
                              .build();
  }

}
