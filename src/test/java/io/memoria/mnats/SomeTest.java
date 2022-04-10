package io.memoria.mnats;

import io.vavr.collection.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.time.Duration;

class SomeTest {
  @Test
  void longPolling() {
    var f = Flux.generate((SynchronousSink<List<Integer>> sink) -> {
      sink.next(blockingMethod());
    }).delayElements(Duration.ofMillis(1000)).flatMap(Flux::fromIterable);
    f.doOnNext(System.out::println).blockLast();
  }

  private List<Integer> blockingMethod() {
    return List.of(1, 2, 3, 4, 5);
  }
}
