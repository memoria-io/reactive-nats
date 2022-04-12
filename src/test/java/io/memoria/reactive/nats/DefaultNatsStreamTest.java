package io.memoria.reactive.nats;

import io.memoria.reactive.core.id.Id;
import io.memoria.reactive.core.stream.Msg;
import io.memoria.reactive.core.stream.Stream;
import io.nats.client.JetStreamApiException;
import io.vavr.collection.HashSet;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.util.Random;

import static io.nats.client.api.StorageType.Memory;

@TestMethodOrder(OrderAnnotation.class)
class DefaultNatsStreamTest {
  private static final int MSG_COUNT = 1000;
  private static final String STREAM = "default_nats_stream";
  private static final Random r = new Random();
  private static final String TOPIC = "node" + r.nextInt(1000);
  private static final int PARTITION = 0;
  private static final String subject = NatsUtils.toSubject(TOPIC, PARTITION);
  private static final Stream repo;

  static {
    try {
      var config = new NatsConfig("nats://localhost:4222", STREAM, Memory, HashSet.of(subject), 1, 100, 200, 100);
      repo = NatsStream.create(config);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Test
  @Order(1)
  void publish() {
    // Given
    var msgs = Flux.range(0, MSG_COUNT).map(i -> new Msg(Id.of(i), "hello" + i));
    // When
    var pub = repo.publish(TOPIC, PARTITION, msgs);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
  }

  @Test
  @Order(0)
  void sizeBefore() {
    var size = repo.size(TOPIC, PARTITION);
    StepVerifier.create(size).expectNext(0L).verifyComplete();
  }

  @Test
  @Order(2)
  void subscribe() {
    // Given previous publish ran successfully
    // When
    var sub = repo.subscribe(TOPIC, PARTITION, 0).take(MSG_COUNT);
    // Given
    StepVerifier.create(sub).expectNextCount(MSG_COUNT).verifyComplete();
  }
  //
  //  @Test
  //  @Order(3)
  //  void sizeAfter() {
  //    var size = repo.size(TOPIC, PARTITION);
  //    StepVerifier.create(size).expectNext((long) MSG_COUNT).verifyComplete();
  //  }
}
