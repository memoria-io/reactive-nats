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

@TestMethodOrder(OrderAnnotation.class)
class DefaultNatsStreamTest {
  private static final int MSG_COUNT = 100000;
  private static final Random r = new Random();
  private static final String topic = "topic" + r.nextInt(1000);
  private static final int partition = 1;
  private static final Stream repo;

  static {
    try {
      var streams = HashSet.of(TestUtils.streamConfig(topic, partition));
      var config = new Config("nats://localhost:4222", streams);
      repo = NatsStream.create(config);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Test
  @Order(0)
  void t0_sizeBefore() {
    var size = repo.size(topic, partition);
    StepVerifier.create(size).expectNext(0L).verifyComplete();
  }

  @Test
  @Order(1)
  void t1_publish() throws InterruptedException {
    // Given
    var msgs = Flux.range(0, MSG_COUNT).map(i -> new Msg(topic, partition, Id.of(i), "hello" + i));
    // When
    var pub = repo.publish(msgs);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
    Thread.sleep(100);
  }

  @Test
  @Order(2)
  void t2_subscribe() {
    // Given previous publish ran successfully
    var offset = 500;
    // When
    var sub = repo.subscribe(topic, partition, offset).take(MSG_COUNT - offset).map(Msg::id);
    // Given
    StepVerifier.create(sub).expectNextCount(MSG_COUNT - offset).verifyComplete();
  }

  @Test
  @Order(3)
  void t3_sizeAfter() {
    var size = repo.size(topic, partition);
    StepVerifier.create(size).expectNext((long) MSG_COUNT).verifyComplete();
  }
}
