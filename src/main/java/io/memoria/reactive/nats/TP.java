package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Message;
import io.vavr.Tuple;
import io.vavr.Tuple2;

public record TP(String topic, int partition) {
  public static final String SPLIT_TOKEN = "_";
  public static final String SUBJECT = ".subject";

  public TP {
    validateName(topic, partition);
  }

  public String streamName() {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }

  public String subjectName() {
    return streamName() + SUBJECT;
  }

  private void validateName(String name, int partition) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Name can't be null or empty string");
    }
    if (partition < 0) {
      throw new IllegalArgumentException("Partition can't be less than 0");
    }
  }

  public static TP create(String topic, int partition) {
    return new TP(topic, partition);
  }

  public static TP fromMessage(Message message) {
    var streamName = message.metaData().getStream();
    var tup = topicPartition(streamName);
    return new TP(tup._1, tup._2);
  }

  public static TP fromMsg(Msg msg) {
    return new TP(msg.topic(), msg.partition());
  }

  public static TP fromStreamName(String streamName) {
    var tup = topicPartition(streamName);
    return new TP(tup._1, tup._2);
  }

  private static Tuple2<String, Integer> topicPartition(String streamName) {
    var s = streamName.split(SPLIT_TOKEN);
    var topic = s[0];
    var partition = Integer.parseInt(s[1]);
    return Tuple.of(topic, partition);
  }
}
