package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PushSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static io.memoria.reactive.nats.NatsStream.MSG_ID_HEADER;

class NatsUtils {

  private NatsUtils() {}

  public static StreamInfo createOrUpdateStream(Connection nc, StreamConfiguration options)
          throws IOException, JetStreamApiException {
    var streamNames = nc.jetStreamManagement().getStreamNames();
    if (streamNames.contains(options.getName()))
      return nc.jetStreamManagement().updateStream(options);
    else
      return nc.jetStreamManagement().addStream(options);
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
    var subject = NatsUtils.toSubject(topic, partition);
    return streamInfo(nc, topic).map(StreamInfo::getStreamState)
                                .map(StreamState::getSubjects)
                                .flatMap(Option::of)
                                .map(List::ofAll)
                                .getOrElse(List::empty)
                                .find(s -> s.getName().equalsIgnoreCase(subject))
                                .map(Subject::getCount)
                                .getOrElse(0L);
  }

  public static Msg toMsg(String topic, int partition, Message message) {
    var id = Id.of(message.getHeaders().getFirst(MSG_ID_HEADER));
    var value = new String(message.getData(), StandardCharsets.UTF_8);
    return new Msg(topic, partition, id, value);
  }

  public static Message toMessage(Msg msg) {
    var subject = toSubject(msg.topic(), msg.partition());
    var headers = new Headers();
    headers.add(MSG_ID_HEADER, msg.id().value());
    var data = msg.value().getBytes(StandardCharsets.UTF_8);
    return NatsMessage.builder().subject(subject).headers(headers).data(data).build();
  }

  public static String toSubject(String topic, int partition) {
    return "%s%s%d".formatted(topic, NatsStream.TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }
}
