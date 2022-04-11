package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.PublishOptions;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import static io.nats.client.api.StorageType.Memory;

class NatsUtilsTest {
  private static final String stream = "nats_utils_stream";
  private static final Random r = new Random();
  private static final String subject = NatsUtils.toSubject("node" + r.nextInt(1000), 0);
  private static Connection nc;
  private static JetStream js;

  @BeforeAll
  static void beforeAll() throws IOException, InterruptedException, JetStreamApiException {
    nc = Nats.connect("nats://localhost:4222");
    js = nc.jetStream();
    var streamOptions = StreamConfiguration.builder().name(stream).addSubjects(subject).storageType(Memory).build();
    NatsUtils.createOrUpdateStream(nc, streamOptions);
  }

  @Test
  void size() throws JetStreamApiException, IOException {
    var size = NatsUtils.subjectSize(nc, subject);
    Assertions.assertEquals(0, size);
  }

  @Test
  void publish() throws ExecutionException, InterruptedException {
    var msg = NatsMessage.builder().subject(subject).data("hello world".getBytes(StandardCharsets.UTF_8)).build();
    var pubOpts = PublishOptions.builder().messageId("msg_id_0").stream(stream).build();
    var ack = NatsUtils.publish(js, msg, pubOpts).get();
    Assertions.assertEquals(1, ack.getSeqno());
  }
}
