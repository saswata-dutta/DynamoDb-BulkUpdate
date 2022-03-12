package org.saswata;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class App {
    public static void main(String[] args) {
        final Semaphore permits = new Semaphore(3);
        final List<Integer> failures = Collections.synchronizedList(new ArrayList<>());

        IntStream.range(0, 7).forEach(id ->
                processAsync("", id, null, permits, failures)
        );

        sleep(6);
        synchronized (failures) {
            log("Failures : " + failures);
        }
    }

    private static CompletableFuture<Boolean> noop() {
        return CompletableFuture.completedFuture(false);
    }

    private static CompletableFuture<Boolean> done() {
        return CompletableFuture.completedFuture(true);
    }

    private synchronized static void log(final String message) {
        System.out.println("[" + Thread.currentThread().getName() + "]\t" + LocalTime.now() + " -> " + message);
    }

    private synchronized static void log(final Throwable e) {
        System.err.println("[" + Thread.currentThread().getName() + "]\t" + LocalTime.now() + " -> " + e.getMessage());
    }

    private static void processAsync(final String fileId, final int val, final String[] lines,
                                     final Semaphore permits,
                                     final List<Integer> failures) {
        checkPointFind("", val)
                .thenCompose(found ->
                        batchUpdate(found, lines, val, 3))
                .thenCompose(updated -> checkPointUpdate(updated, "", val))
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log(ex);
                        failures.add(val);
                    } else {
                        log("Work Done = " + result + " id : " + val);
                    }
                });
    }

    private static CompletableFuture<Boolean> checkPointFind(final String fileId, final int val) {
        return CompletableFuture
                .supplyAsync(() -> {
                    log("Started id : " + val);
                    sleep(1);
                    if (val == 1)
                        throw new RuntimeException("Failed checkPointExists id : " + val);
                    boolean exists = val == 0;
                    log("checkPointExists " + exists + " id : " + val);
                    return exists;
                });
    }

    private static CompletableFuture<Boolean> batchUpdate(final boolean checkPointFound,
                                                          final String[] lines, final int val,
                                                          final int retriesRemaining) {
        if (retriesRemaining < 1) {
            throw new RuntimeException("Retries Exhausted id : " + val);
        }

        if (checkPointFound) {
            log("batchUpdate skipped id : " + val);
            return noop();
        }

        return CompletableFuture
                .supplyAsync(() -> {
                    sleep(1);
                    log("batchUpdate retriesRemaining " + retriesRemaining + " id : " + val);
                    if (val == 2) throw new RuntimeException("Failed batchUpdate id : " + val);
                    return lines;
                }).thenCompose(x -> retriesRemaining > 1 || val == 3 ?
                        batchUpdate(false, x, val, retriesRemaining - 1) : done()
                );
    }

    private static CompletableFuture<Boolean> checkPointUpdate(final boolean batchUpdated,
                                                               final String fileId, final int val) {
        if (!batchUpdated) {
            log("checkPointUpdate skipped id : " + val);
            return noop();
        }

        return CompletableFuture
                .supplyAsync(() -> {
                    sleep(1);
                    if (val == 4) throw new RuntimeException("Failed checkPointUpdate id : " + val);
                    log("checkPointUpdate id : " + val);
                    return true;
                });
    }

    private static void sleep(final int seconds) {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(seconds));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
