package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.api.StreamState;
import io.nats.client.api.Subject;
import io.vavr.collection.List;
import io.vavr.control.Option;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

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

  public static int getPartition(String subject) {
    var lastIdx = subject.lastIndexOf(NatsStream.TOPIC_PARTITION_SPLIT_TOKEN);
    var partition = 0;
    if (lastIdx > -1) {
      partition = Integer.parseInt(subject.substring(lastIdx + 1));
    }
    return partition;
  }

  public static CompletableFuture<PublishAck> publish(JetStream js,
                                                      String subject,
                                                      String message,
                                                      PublishOptions pubOpt) {
    return js.publishAsync(subject, message.getBytes(StandardCharsets.UTF_8), pubOpt);
  }

  public static JetStreamSubscription pullSubscription(JetStream js, String topic, int partition, long offset)
          throws IOException, JetStreamApiException {
    var subject = toSubject(topic, partition);
    var consumerName = "%s_consumer".formatted(subject);
    var cc = ConsumerConfiguration.builder().durable(consumerName).startSequence(offset).build();
    var pullOptions = PullSubscribeOptions.builder().configuration(cc).build();
    return js.subscribe(subject, pullOptions);
  }

  public static Option<StreamInfo> streamInfo(Connection nc, String streamName)
          throws IOException, JetStreamApiException {
    try {
      return Option.some(nc.jetStreamManagement().getStreamInfo(streamName));
    } catch (JetStreamApiException e) {
      if (e.getErrorCode() == 404) {
        return Option.none();
      } else {
        throw e;
      }
    }
  }

  public static long subjectSize(Connection nc, String subjectName) throws IOException, JetStreamApiException {
    return streamInfo(nc, subjectName).map(StreamInfo::getStreamState)
                                      .map(StreamState::getSubjects)
                                      .map(List::ofAll)
                                      .getOrElse(List::empty)
                                      .find(s -> s.getName().equalsIgnoreCase(subjectName))
                                      .map(Subject::getCount)
                                      .getOrElse(0L);
  }

  public static Msg toMsg(Message msg) {
    return new Msg(Id.of(msg.metaData().streamSequence()), new String(msg.getData(), StandardCharsets.UTF_8));
  }

  //  public static Message toNatsMsg(Msg msg) {
  //    return NatsMessage.builder().subject(subject).data(msg.value()).build();
  //  }

  public static String toSubject(String topic, int partition) {
    return "%s%s%d".formatted(topic, NatsStream.TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }
}
