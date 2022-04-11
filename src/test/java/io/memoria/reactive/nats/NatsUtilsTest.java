package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;

class NatsUtilsTest {
  private static final Random random = new Random();
  private static final int MSG_COUNT = 1000;
  private static final String TOPIC = "node" + random.nextInt(1000);
  private static final int PARTITION = 0;
  private static final Connection nc;
  private static final JetStream js;

  static {
    try {
      var config = new NatsConfig("nats://localhost:4222", "nats_mem_stream", 1, StreamStorage.MEMORY, 100, 200, 100);
      nc = Nats.connect(config.url());
      js = nc.jetStream();
    } catch (IOException | InterruptedException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Test
  void size() throws JetStreamApiException, IOException {
    var subj = NatsUtils.toSubject(TOPIC, PARTITION);
    var size = NatsUtils.subjectSize(nc, subj);
    Assertions.assertEquals(0, size);
  }
}
