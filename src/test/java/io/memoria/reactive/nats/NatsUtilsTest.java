package io.memoria.reactive.nats;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.NatsMessage;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.Random;

import static io.nats.client.api.StorageType.File;

@TestMethodOrder(OrderAnnotation.class)
class NatsUtilsTest {
  private static final int MSG_COUNT = 1000;
  private static final String stream = "file_stream";
  private static final Random r = new Random();
  private static final String subject = NatsUtils.toSubject("node" + r.nextInt(1000), 0);
  private static Connection nc;
  private static JetStream js;

  @BeforeAll
  static void beforeAll() throws IOException, InterruptedException, JetStreamApiException {
    nc = Nats.connect("nats://localhost:4222");
    js = nc.jetStream();
    var streamOptions = StreamConfiguration.builder().name(stream).addSubjects(subject).storageType(File).build();
    NatsUtils.createOrUpdateStream(nc, streamOptions);
  }

  @Test
  @Order(0)
  void streamInfo() throws JetStreamApiException, IOException {
    var info = NatsUtils.streamInfo(nc, stream).get();
    Assertions.assertTrue(info.getConfiguration().getSubjects().contains(subject));
  }

  @Test
  @Order(1)
  void sizeBefore() throws JetStreamApiException, IOException {
    var size = NatsUtils.subjectSize(nc, stream, subject);
    Assertions.assertEquals(0, size);
  }

  @Test
  @Order(2)
  void publish() {
    var msgs = List.range(0, MSG_COUNT).map(i -> NatsMessage.builder().subject(subject).data("hello" + i).build());
    msgs.forEach(msg -> Try.of(() -> js.publish(msg)).get());
  }

  @Test
  @Order(3)
  void sizeAfter() throws JetStreamApiException, IOException {
    var size = NatsUtils.subjectSize(nc, stream, subject);
    Assertions.assertEquals(MSG_COUNT, size);
  }
}
