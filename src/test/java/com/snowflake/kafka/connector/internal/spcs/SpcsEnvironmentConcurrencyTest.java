package com.snowflake.kafka.connector.internal.spcs;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Concurrency cover for ambient SPCS authentication.
 *
 * <p>Every other test in this area is single threaded, but the connector is not. Kafka Connect runs
 * {@code tasks.max} sink tasks in one worker JVM, each building its own connection and its own
 * streaming client, so {@link SpcsEnvironment} is called from many threads at once. Worse, the
 * token rotates underneath those calls: a rotation was measured at roughly a ten minute interval on
 * a live service, which is far shorter than the lifetime of a running connector. These tests pin
 * the two properties that follow from that.
 *
 * <p><b>On modeling rotation faithfully.</b> The rotator here replaces the token with {@link
 * StandardCopyOption#ATOMIC_MOVE} rather than by writing over the existing file. That is
 * deliberate, and it is not merely a convenience to keep the test quiet. Kubernetes projected
 * volumes, which is what SPCS mounts the token through, publish a new version into a hidden
 * timestamped directory and then atomically swing a symlink, so a reader never observes a partially
 * written or truncated file. Writing in place would instead create a window in which the file is
 * zero length, which the empty token guard correctly rejects. A test that failed that way would be
 * reporting an artifact of its own file handling rather than anything about the connector.
 */
public class SpcsEnvironmentConcurrencyTest {

  private static final String HOST = "myaccount.us-east-1.snowflakecomputing.com";
  private static final int THREADS = 8;
  private static final int READS_PER_THREAD = 250;
  private static final int MAX_READS_PER_THREAD = 200_000;

  @TempDir Path tempDir;

  @AfterEach
  void reset() {
    SpcsEnvironment.resetForTests();
  }

  private Path simulateSpcs(String tokenValue) throws IOException {
    Path token = tempDir.resolve("token");
    Files.write(token, tokenValue.getBytes(StandardCharsets.UTF_8));
    Map<String, String> env = new HashMap<>();
    env.put(SpcsEnvironment.ENV_HOST, HOST);
    env.put(SpcsEnvironment.ENV_DATABASE, "AMBIENT_DB");
    env.put(SpcsEnvironment.ENV_SCHEMA, "AMBIENT_SCHEMA");
    SpcsEnvironment.overrideForTests(env::get, token);
    return token;
  }

  /** Replaces the token atomically, the way a Kubernetes projected volume does. */
  private void rotateAtomically(Path token, String newValue) throws IOException {
    Path staged = tempDir.resolve("token.staged");
    Files.write(staged, newValue.getBytes(StandardCharsets.UTF_8));
    Files.move(staged, token, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Many concurrent readers, with the token rotating underneath them, must every one of them get a
   * whole and current token. This is the property the connector depends on when a rotation lands in
   * the middle of a batch: the token is re-read per use rather than cached, so no task may see a
   * stale or partial value.
   *
   * <p>The readers keep going until the rotator has finished its fixed set of rotations, rather
   * than running a fixed number of iterations. That ordering matters: with a fixed iteration count,
   * a caching implementation returns without touching the disk and the readers can finish before
   * the first rotation lands, so the test would fail on "the token really did rotate" instead of on
   * the caching assertion that is the actual subject. Tying the readers to the rotator's progress
   * means the failure names the real cause.
   */
  @Test
  void shouldReadWholeCurrentTokenFromManyThreadsWhileItRotates() throws Exception {
    final int plannedRotations = 5;
    Path token = simulateSpcs("tok-0");
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    Set<String> observed = ConcurrentHashMap.newKeySet();
    AtomicBoolean rotatorDone = new AtomicBoolean(false);
    AtomicInteger rotations = new AtomicInteger();
    CountDownLatch ready = new CountDownLatch(THREADS);
    CountDownLatch go = new CountDownLatch(1);

    ExecutorService pool = Executors.newFixedThreadPool(THREADS);
    Thread rotator =
        new Thread(
            () -> {
              try {
                for (int n = 1; n <= plannedRotations; n++) {
                  Thread.sleep(15);
                  rotateAtomically(token, "tok-" + n);
                  rotations.incrementAndGet();
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                failures.add(e);
              } finally {
                rotatorDone.set(true);
              }
            });
    rotator.setDaemon(true);

    try {
      for (int t = 0; t < THREADS; t++) {
        pool.submit(
            () -> {
              ready.countDown();
              try {
                go.await();
                int reads = 0;
                while (!rotatorDone.get() || reads < READS_PER_THREAD) {
                  observed.add(SpcsEnvironment.readToken());
                  if (++reads > MAX_READS_PER_THREAD) {
                    break;
                  }
                }
              } catch (Throwable e) {
                failures.add(e);
              }
            });
      }

      assertThat(ready.await(10, TimeUnit.SECONDS)).as("readers started").isTrue();
      rotator.start();
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).as("readers finished").isTrue();
      rotator.join(TimeUnit.SECONDS.toMillis(30));
    } finally {
      rotatorDone.set(true);
      pool.shutdownNow();
    }

    assertThat(failures).as("no reader saw an error while the token rotated").isEmpty();
    assertThat(rotations.get())
        .as("the rotator completed every planned rotation")
        .isEqualTo(plannedRotations);
    assertThat(observed)
        .as("every value read was a whole token, never empty or partial")
        .isNotEmpty()
        .allSatisfy(value -> assertThat(value).matches("tok-\\d+"));
    assertThat(observed)
        .as("readers saw more than one generation, so the token is not cached")
        .hasSizeGreaterThan(1);
  }

  /**
   * Each sink task resolves its own configuration. Resolution must therefore be free of shared
   * mutable state: one task's configuration must never leak into another's. A single shared map or
   * a cached result would show up here as cross contamination.
   */
  @Test
  void shouldResolveFromManyThreadsWithoutCrossContamination() throws Exception {
    simulateSpcs("tok");
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);

    try {
      for (int t = 0; t < THREADS; t++) {
        final int id = t;
        pool.submit(
            () -> {
              try {
                go.await();
                for (int i = 0; i < 200; i++) {
                  Map<String, String> raw = new HashMap<>();
                  raw.put("topics", "topic-" + id);
                  raw.put(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ROLE_" + id);

                  Map<String, String> resolved = SpcsEnvironment.resolve(raw);

                  assertThat(resolved)
                      .containsEntry("topics", "topic-" + id)
                      .containsEntry(KafkaConnectorConfigParams.SNOWFLAKE_ROLE_NAME, "ROLE_" + id)
                      .containsEntry(
                          KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
                          AuthenticatorType.SPCS.toConfigValue())
                      .containsEntry(
                          KafkaConnectorConfigParams.SNOWFLAKE_USER_NAME,
                          SpcsEnvironment.AMBIENT_USER_PLACEHOLDER)
                      .containsEntry(
                          KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME, "AMBIENT_DB");
                  assertThat(resolved.get(KafkaConnectorConfigParams.SNOWFLAKE_URL_NAME))
                      .contains(HOST);
                }
              } catch (Throwable e) {
                failures.add(e);
              }
            });
      }
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).as("resolvers finished").isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(failures).as("no resolver saw an error or foreign configuration").isEmpty();
  }

  /**
   * Detection is called on every task startup and must not be sensitive to being called
   * concurrently. A flapping answer here would make some tasks adopt ambient authentication while
   * their siblings did not, which is the worst possible outcome: a partially authenticated
   * connector that half works.
   */
  @Test
  void shouldReportInsideSpcsConsistentlyUnderConcurrency() throws Exception {
    simulateSpcs("tok");
    Set<Boolean> answers = ConcurrentHashMap.newKeySet();
    List<Throwable> failures = new CopyOnWriteArrayList<>();
    CountDownLatch go = new CountDownLatch(1);
    ExecutorService pool = Executors.newFixedThreadPool(THREADS);

    try {
      for (int t = 0; t < THREADS; t++) {
        pool.submit(
            () -> {
              try {
                go.await();
                for (int i = 0; i < READS_PER_THREAD; i++) {
                  answers.add(SpcsEnvironment.isInsideSpcs());
                }
              } catch (Throwable e) {
                failures.add(e);
              }
            });
      }
      go.countDown();
      pool.shutdown();
      assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).as("detectors finished").isTrue();
    } finally {
      pool.shutdownNow();
    }

    assertThat(failures).isEmpty();
    assertThat(answers).as("detection never disagreed across threads").containsExactly(true);
  }
}
