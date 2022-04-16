package io.memoria.reactive.nats;

import io.memoria.reactive.core.stream.Msg;

public record TP(String topic, int partition) {
  public static final String SPLIT_TOKEN = "_";
  public static final String SUBJECT_EXT = ".subject";

  public TP {
    validateName(topic, partition);
  }

  public String streamName() {
    return "%s%s%d".formatted(topic, SPLIT_TOKEN, partition);
  }

  public String subjectName() {
    return streamName() + SUBJECT_EXT;
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

  public static TP fromMsg(Msg msg) {
    return new TP(msg.topic(), msg.partition());
  }

  public static TP fromSubject(String subject) {
    var idx = subject.indexOf(SUBJECT_EXT);
    var s = subject.substring(0, idx).split(SPLIT_TOKEN);
    var topic = s[0];
    var partition = Integer.parseInt(s[1]);
    return TP.create(topic, partition);
  }
}
