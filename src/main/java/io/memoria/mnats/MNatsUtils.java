package io.memoria.mnats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Message;

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

  static int getPartition(String subject) {
    var lastIdx = subject.lastIndexOf(TOPIC_PARTITION_SPLIT_TOKEN);
    var partition = 0;
    if (lastIdx > -1) {
      partition = Integer.parseInt(subject.substring(lastIdx));
    }
    return partition;
  }

//  public static long topicSize(String topic, int partition, Map<String, Object> conf) {
  //    var consumer = new KafkaConsumer<Long, String>(conf.toJavaMap());
  //    var tp = new TopicPartition(topic, partition);
  //    var tpCol = List.of(tp).toJavaList();
  //    consumer.assign(tpCol);
  //    consumer.seekToEnd(tpCol);
  //    return consumer.position(tp);
  //  }

  static String toSubject(String topic, int partition) {
    return "%s%s%d".formatted(topic, TOPIC_PARTITION_SPLIT_TOKEN, partition);
  }
}
