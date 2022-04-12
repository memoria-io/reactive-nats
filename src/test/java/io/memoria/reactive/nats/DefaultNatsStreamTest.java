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

import static io.nats.client.api.StorageType.File;

@TestMethodOrder(OrderAnnotation.class)
class DefaultNatsStreamTest {
  private static final int MSG_COUNT = 100000;
  private static final String STREAM = "file_nats_stream";
  private static final Random r = new Random();
  private static final String topic = "topic" + r.nextInt(1000);
  private static final int partition = 0;
  private static final String subject = NatsUtils.toSubject(topic, partition);
  private static final Stream repo;

  static {
    try {
      var config = new NatsConfig("nats://localhost:4222", STREAM, File, HashSet.of(subject), 1, 100, 100);
      repo = NatsStream.create(config);
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Test
  @Order(0)
  void sizeBefore() {
    var size = repo.size(topic, partition);
    StepVerifier.create(size).expectNext(0L).verifyComplete();
  }

  @Test
  @Order(1)
  void publish() {
    // Given
    var msgs = Flux.range(0, MSG_COUNT).map(i -> new Msg(topic, partition, Id.of(i), "hello" + i));
    // When
    var pub = repo.publish(msgs);
    // Then
    StepVerifier.create(pub).expectNextCount(MSG_COUNT).verifyComplete();
  }

  //  @Test
  //  @Order(2)
  //  void subscribe() {
  //    // Given previous publish ran successfully
  //    // When
  //    var sub = repo.subscribe(TOPIC, PARTITION, 0).take(MSG_COUNT).doOnNext(System.out::println);
  //    // Given
  //    StepVerifier.create(sub).expectNextCount(MSG_COUNT).verifyComplete();
  //  }

  @Test
  @Order(3)
  void sizeAfter() {
    var size = repo.size(topic, partition);
    StepVerifier.create(size).expectNext((long) MSG_COUNT).verifyComplete();
  }
}
