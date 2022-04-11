package io.memoria.reactive.nats;

import io.nats.client.JetStreamApiException;
import io.nats.client.Nats;
import io.vavr.collection.List;
import io.vavr.control.Option;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.time.Duration;

class SomeTest {
  @Test
  void longPolling() {
    var f = Flux.generate((SynchronousSink<List<Integer>> sink) -> {
      sink.next(blockingMethod());
    }).delayElements(Duration.ofMillis(1000)).flatMap(Flux::fromIterable);
    f.doOnNext(System.out::println).blockLast();
  }
  
  @Test
  void vavr(){
    var r = List.<Integer>empty().find(s-> s==1).getOrElse(0);
    System.out.println(r);
  }
  
  @Test
  void nats(){
    try {
      var config = new NatsConfig("nats://localhost:4222", "nats_mem_stream", 1, StreamStorage.MEMORY, 100, 200, 100);
      var nc = Nats.connect(config.url());
      
      var info = nc.jetStreamManagement().getStreamInfo(config.streamName());
      System.out.println(info);
      
    } catch (IOException | InterruptedException | JetStreamApiException e) {
      e.printStackTrace();
    }
  }

  private List<Integer> blockingMethod() {
    return List.of(1, 2, 3, 4, 5);
  }
}
