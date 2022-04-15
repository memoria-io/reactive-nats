package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.impl.NatsMessage;
import io.vavr.control.Try;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UtilsTest {
  private static final Connection nc = Try.of(() -> Nats.connect("nats://localhost:4222")).get();
  private static final JetStream js = Try.of(nc::jetStream).get();

  @Test
  void toMessage() {
    var message = Utils.toMessage(new Msg("topic", 0, Id.of(1000), "hello world"));
    Assertions.assertEquals(message.getHeaders().getFirst(Utils.MSG_ID_HEADER), "1000");
    Assertions.assertEquals(message.getSubject(), "topic_0.stream");
  }

  @Test
  void toMsg() {
    
  }
}
