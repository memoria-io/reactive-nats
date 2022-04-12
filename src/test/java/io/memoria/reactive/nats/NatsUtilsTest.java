package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static io.nats.client.api.StorageType.Memory;

class NatsUtilsTest {
  private static final String stream = "nats_utils_stream";
  private static final Random r = new Random();
  private static final String subject = NatsUtils.toSubject("node" + r.nextInt(1000), 0);
  private static Connection nc;
  private static JetStream js;

  @Test
  void publish() throws ExecutionException, InterruptedException {
    var pubOpts = PublishOptions.builder().messageId("msg_id_0").stream(stream).build();
    var ack = NatsUtils.publish(js, subject, "hello world", pubOpts).get();
    Assertions.assertEquals(1, ack.getSeqno());
  }

  @Test
  void size() throws JetStreamApiException, IOException {
    var size = NatsUtils.subjectSize(nc, subject);
    Assertions.assertEquals(0, size);
  }

  @BeforeAll
  static void beforeAll() throws IOException, InterruptedException, JetStreamApiException {
    nc = Nats.connect("nats://localhost:4222");
    js = nc.jetStream();
    var streamOptions = StreamConfiguration.builder().name(stream).addSubjects(subject).storageType(Memory).build();
    NatsUtils.createOrUpdateStream(nc, streamOptions);
  }
}
