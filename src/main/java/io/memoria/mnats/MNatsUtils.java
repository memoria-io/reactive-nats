package io.memoria.mnats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Message;
import io.nats.client.impl.NatsMessage;

import java.nio.charset.StandardCharsets;

import static io.memoria.mnats.NatsStream.TOPIC_PARTITION_SPLIT_TOKEN;

class MNatsUtils {

  private MNatsUtils() {}

  public static Msg toMsg(Message msg) {
    var subject = msg.getSubject();
    int partition = getPartition(subject);
    return new Msg(subject,
                   partition,
                   Id.of(msg.metaData().streamSequence()),
                   new String(msg.getData(), StandardCharsets.UTF_8));
  }

  public static Message toNatsMsg(Msg msg) {
    var subject = toSubject(msg.topic(), msg.partition());
    return NatsMessage.builder().subject(subject).data(msg.value()).build();
  }

  static int getPartition(String subject) {
    var lastIdx = subject.lastIndexOf(TOPIC_PARTITION_SPLIT_TOKEN);
    var partition = 0;
    if (lastIdx > -1) {
      partition = Integer.parseInt(subject.substring(lastIdx));
    }
    return partition;
  }

  static String toSubject(String topic, int partition) {
    return "%s%s%d".formatted(topic, TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }
}
