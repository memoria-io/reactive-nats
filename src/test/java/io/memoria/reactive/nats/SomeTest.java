package io.memoria.reactive.nats;

import io.vavr.collection.List;
import org.junit.jupiter.api.Test;

class SomeTest {
  //  @Test
  //  void longPolling() {
  //    var f = Flux.generate((SynchronousSink<List<Integer>> sink) -> {
  //      sink.next(blockingMethod());
  //    }).delayElements(Duration.ofMillis(1000)).flatMap(Flux::fromIterable);
  //    f.doOnNext(System.out::println).blockLast();
  //  }

  @Test
  void vavr() {
    var r = List.<Integer>empty().find(s -> s == 1).getOrElse(0);
    System.out.println(r);
  }

  private List<Integer> blockingMethod() {
    return List.of(1, 2, 3, 4, 5);
  }
}
