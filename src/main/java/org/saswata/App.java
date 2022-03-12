package org.saswata;

import java.time.LocalTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class App {
    public static void main(String[] args) {
        IntStream.range(0, 5).forEach(id ->
                checkPointExists("", id)
                        .thenCompose(exists ->
                                exists ?
                                        noop() :
                                        batchUpdate(null, 3))
                        .thenCompose(nil -> checkPointUpdate("", id))
        );

        sleep(11);
    }

    private static final boolean INSIDE_BOOM0 = false;
    private static final boolean INSIDE_BOOM1 = false;
    private static final boolean INSIDE_BOOM2 = false;
    private static final boolean INSIDE_BOOM3 = false;

    private static CompletableFuture<Void> noop() {
        return CompletableFuture.completedFuture(null);
    }

    private static void log(String message) {
        System.out.println("[" + Thread.currentThread().getName() + "]\t" + LocalTime.now() + " -> " + message);
    }

    private static CompletableFuture<Boolean> checkPointExists(String fileId, int val) {
        return CompletableFuture
                .supplyAsync(() -> {
                    sleep(2);
                    if (INSIDE_BOOM0) throw new RuntimeException("Failed checkPointExists " + fileId + val);
                    log("checkPointExists " + val);
                    return false;
                });
    }

    private static CompletableFuture<Void> batchUpdate(String[] lines, int retriesRemaining) {
        if (retriesRemaining < 1) {
            throw new RuntimeException("Retries Exhausted");
        }

        return CompletableFuture
                .supplyAsync(() -> {
                    sleep(2);
                    log("batchUpdate attempt id : " + retriesRemaining);
                    if (INSIDE_BOOM1) throw new RuntimeException("Failed batchUpdate");
                    return lines;
                }).thenCompose(x -> !INSIDE_BOOM3 && retriesRemaining == 1 ?
                        noop() :
                        batchUpdate(x, retriesRemaining - 1));
    }

    private static CompletableFuture<Void> checkPointUpdate(String fileId, int val) {
        return CompletableFuture
                .runAsync(() -> {
                    sleep(2);
                    if (INSIDE_BOOM2) throw new RuntimeException("Failed batchUpdate");
                    log("checkPointUpdate " + val);
                });
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
