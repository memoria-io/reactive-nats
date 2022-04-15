package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {
  @Test
  void toMsg() {
    
  }

  @Test
  void toMessage() {
    var message = Utils.toMessage(new Msg("topic", 0, Id.of(1000), "hello world"));
    Assertions.assertEquals(message.getHeaders().getFirst(Utils.MSG_ID_HEADER), "1000");
    Assertions.assertEquals(message.getSubject(), "topic_0.stream");
  }

  @Test
  void toStreamName() {
    var actual = Utils.toStreamName("topic", 0);
    Assertions.assertEquals("topic_0", actual);
  }

  @Test
  void toSubjectName() {
    var actual = Utils.toSubjectName("stream");
    Assertions.assertEquals("stream.stream", actual);
  }
}
