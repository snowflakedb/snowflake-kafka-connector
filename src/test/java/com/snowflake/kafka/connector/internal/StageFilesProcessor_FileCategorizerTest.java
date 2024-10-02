package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Functions;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.snowflake.client.jdbc.internal.joda.time.DateTime;
import net.snowflake.client.jdbc.internal.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class StageFilesProcessor_FileCategorizerTest {
  @Test
  void allFilesWillBeCategorizedAsStageFilesWhenNoInitialOffsetIsSupplied() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files =
        IntStream.rangeClosed(0, 9)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() + Duration.ofSeconds(i).toMillis()))
            .collect(Collectors.toList());

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);

    assertThat(victim.hasDirtyFiles()).isFalse();
    assertThat(victim.hasStageFiles()).isTrue();
    assertThat(victim.query(t -> true).collect(Collectors.toList())).containsAll(files);
  }

  @Test
  void filesOlderThen1MinuteWillBeCategorizedAsMature() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files = new ArrayList<>();
    String matureFile =
        String.format(
            "connector/topic/0/1_10_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(61).toMillis());
    String justMaturedFile =
        String.format(
            "connector/topic/0/11_19_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(60).toMillis());
    String toYoungFile =
        String.format(
            "connector/topic/0/20_29_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(59).toMillis());

    files.add(matureFile);
    files.add(justMaturedFile);
    files.add(toYoungFile);

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);

    assertThat(victim.hasDirtyFiles()).isFalse();
    assertThat(victim.hasStageFiles()).isTrue();
    assertThat(victim.query(filters.matureFilePredicate).collect(Collectors.toList()))
        .containsOnly(matureFile, justMaturedFile);
  }

  @Test
  void filesYoungerThen1MinuteWillNotBeCategorizedAsLoaded() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files = new ArrayList<>();
    String matureFile =
        String.format(
            "connector/topic/0/1_10_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(59).toMillis());
    String justMaturedFile =
        String.format(
            "connector/topic/0/11_19_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(58).toMillis());
    String toYoungFile =
        String.format(
            "connector/topic/0/20_29_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(57).toMillis());

    files.add(matureFile);
    files.add(justMaturedFile);
    files.add(toYoungFile);

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);
    Map<String, StageFilesProcessor.IngestEntry> statuses =
        files.stream()
            .collect(
                Collectors.toMap(
                    Functions.identity(),
                    fileName ->
                        new StageFilesProcessor.IngestEntry(
                            InternalUtils.IngestedFileStatus.LOADED, ts.getMillis())));
    victim.updateFileStatus(statuses);

    assertThat(victim.hasDirtyFiles()).isFalse();
    assertThat(victim.hasStageFiles()).isTrue();
    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.trackableFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
  }

  @Test
  void filesYoungerThen1MinuteWillNotBeCategorizedAsFailed() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files = new ArrayList<>();
    String matureFile =
        String.format(
            "connector/topic/0/1_10_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(59).toMillis());
    String justMaturedFile =
        String.format(
            "connector/topic/0/11_19_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(58).toMillis());
    String toYoungFile =
        String.format(
            "connector/topic/0/20_29_%d.json.gz",
            ts.getMillis() - Duration.ofSeconds(57).toMillis());

    files.add(matureFile);
    files.add(justMaturedFile);
    files.add(toYoungFile);

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);
    Map<String, StageFilesProcessor.IngestEntry> statuses =
        files.stream()
            .collect(
                Collectors.toMap(
                    Functions.identity(),
                    fileName ->
                        new StageFilesProcessor.IngestEntry(
                            InternalUtils.IngestedFileStatus.FAILED, ts.getMillis())));
    victim.updateFileStatus(statuses);

    assertThat(victim.hasDirtyFiles()).isFalse();
    assertThat(victim.hasStageFiles()).isTrue();
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.trackableFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
  }

  @Test
  void allMatureFilesWillBeCategorizedAsDirtyFilesIfOffsetIsSmallerThanLastSubmittedFile() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files =
        IntStream.rangeClosed(1, 10)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() - Duration.ofMinutes(i).toMillis()))
            .collect(Collectors.toList());

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, -1, filters);

    assertThat(victim.hasDirtyFiles()).isTrue();
    assertThat(victim.hasStageFiles()).isFalse();
    assertThat(victim.query(t -> true).collect(Collectors.toList())).isEmpty();
  }

  @Test
  void allFreshFilesWillBeCategorizedAsStageFilesIfTheyHaventMatured() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(() -> ts.toInstant().getMillis(), "");
    List<String> files =
        IntStream.rangeClosed(1, 10)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() + Duration.ofSeconds(i).toMillis()))
            .collect(Collectors.toList());

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, -1, filters);

    assertThat(victim.hasDirtyFiles()).isFalse();
    assertThat(victim.hasStageFiles()).isTrue();
    assertThat(victim.query(t -> true).collect(Collectors.toList())).hasSize(10).containsAll(files);
  }

  @Test
  void willCategorizeAllFilesAsLoadedWhenMatchingStatusIsProvided() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.TimeSupplier timeSupplier =
        () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "");

    List<String> files =
        IntStream.rangeClosed(0, 9)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() - (10 - Duration.ofMinutes(i).toMillis())))
            .collect(Collectors.toList());

    Map<String, StageFilesProcessor.IngestEntry> statuses =
        files.stream()
            .collect(
                Collectors.toMap(
                    Functions.identity(),
                    fileName ->
                        new StageFilesProcessor.IngestEntry(
                            InternalUtils.IngestedFileStatus.LOADED, ts.getMillis())));

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);
    victim.updateFileStatus(statuses);

    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
  }

  @Test
  void willCategorizeAllFilesAsFailedWhenMatchingStatusIsProvided() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.TimeSupplier timeSupplier =
        () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "");

    List<String> files =
        IntStream.rangeClosed(0, 9)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() + Duration.ofMinutes(i).toMillis()))
            .collect(Collectors.toList());

    InternalUtils.IngestedFileStatus[] failedStatus =
        new InternalUtils.IngestedFileStatus[] {
          InternalUtils.IngestedFileStatus.FAILED, InternalUtils.IngestedFileStatus.PARTIALLY_LOADED
        };
    AtomicInteger idx = new AtomicInteger(0);

    Map<String, StageFilesProcessor.IngestEntry> statuses =
        files.stream()
            .collect(
                Collectors.toMap(
                    Functions.identity(),
                    fileName ->
                        new StageFilesProcessor.IngestEntry(
                            failedStatus[idx.getAndIncrement() % failedStatus.length],
                            ts.getMillis())));

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);
    victim.updateFileStatus(statuses);

    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
  }

  @Test
  void willCategorizeAllFilesAsFailedWhenNoStatusIsProvidedButAllFilesAreOlderThanOneHour() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.TimeSupplier timeSupplier =
        () -> ts.getMillis() + Duration.ofMinutes(101L).toMillis();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "");

    List<String> files =
        IntStream.rangeClosed(0, 9)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() + Duration.ofMinutes(i).toMillis()))
            .collect(Collectors.toList());

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);

    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
  }

  @Test
  void
      willCategorizeAllFilesAsStaleWhenNoStatusIsProvidedButAllFilesAreOlderThanMinutesButNotOlderThanOneHour() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.TimeSupplier timeSupplier =
        () -> ts.getMillis() + Duration.ofMinutes(45L).toMillis();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "");

    List<String> files =
        IntStream.rangeClosed(0, 9)
            .mapToObj(
                i ->
                    String.format(
                        "connector/topic/0/%d_%d_%d.json.gz",
                        i * 100,
                        ((i + 1) * 100) - 1,
                        ts.getMillis() + Duration.ofMinutes(10L + i).toMillis()))
            .collect(Collectors.toList());

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);

    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList()))
        .containsAll(files);
    ;
  }

  @Test
  void onlyFilesMarkedAsTrackableWillBeProvided() {
    DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
    StageFilesProcessor.TimeSupplier timeSupplier =
        () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "");

    ArrayList<String> files = new ArrayList<>();
    Map<String, InternalUtils.IngestedFileStatus> statuses = new HashMap<>();
    // old file, but loaded
    files.add(0, String.format("connector/topic/0/1_9_%d.json.gz", ts.getMillis()));
    statuses.put(files.get(0), InternalUtils.IngestedFileStatus.LOADED);
    // old file and failed
    files.add(0, String.format("connector/topic/0/10_19_%d.json.gz", ts.getMillis()));
    statuses.put(files.get(0), InternalUtils.IngestedFileStatus.FAILED);
    // old file and missing status
    files.add(0, String.format("connector/topic/0/20_29_%d.json.gz", ts.getMillis()));
    // stale file, no status
    files.add(
        0,
        String.format(
            "connector/topic/0/30_39_%d.json.gz",
            ts.getMillis() + Duration.ofMinutes(40).toMillis()));
    // fresh file, no status
    files.add(
        0,
        String.format(
            "connector/topic/0/40_49_%d.json.gz",
            ts.getMillis() + Duration.ofMinutes(60).toMillis()));
    // fresh file, failed
    files.add(
        0,
        String.format(
            "connector/topic/0/50_59_%d.json.gz",
            ts.getMillis() + Duration.ofMinutes(60).toMillis()));
    statuses.put(files.get(0), InternalUtils.IngestedFileStatus.PARTIALLY_LOADED);
    // fresh file, loaded
    files.add(
        0,
        String.format(
            "connector/topic/0/60_69_%d.json.gz",
            ts.getMillis() + Duration.ofMinutes(60).toMillis()));
    statuses.put(files.get(0), InternalUtils.IngestedFileStatus.LOADED);

    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE, filters);
    victim.stopTrackingFiles(files);

    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(longs = {Long.MIN_VALUE, Long.MAX_VALUE})
  void fileTestFor_SNOW_1642799(long offset) {
    String fileTimestamp = "28 Aug 2024 @ 17:32:40.822 UTC";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM yyyy '@' HH:mm:ss.SSS z");
    // Parse the string to a ZonedDateTime object
    ZonedDateTime fileTs = ZonedDateTime.parse(fileTimestamp, formatter);
    ArrayList<String> files = new ArrayList<>();
    files.add(
        String.format(
            "lcc_odgzj_2129566559/EDGE_EVENTABLE_ADHOCAVROV1_PROD_PRIVATE/2/576942355_576942400_%d.json.gz",
            fileTs.toInstant().toEpochMilli()));

    Map<String, StageFilesProcessor.IngestEntry> statuses = new HashMap<>();

    String cleanerExecutionTime = "28 Aug 2024 @ 17:32:58.118 UTC";
    ZonedDateTime cleanerRunTime = ZonedDateTime.parse(cleanerExecutionTime, formatter);

    StageFilesProcessor.TimeSupplier timeSupplier = () -> cleanerRunTime.toInstant().toEpochMilli();
    StageFilesProcessor.FilteringPredicates filters =
        new StageFilesProcessor.FilteringPredicates(timeSupplier, "lcc_odgzj_2129566559");
    StageFilesProcessor.FileCategorizer victim =
        StageFilesProcessor.FileCategorizer.build(files, offset, filters);

    String fileUploadTime = "28 Aug 2024 @ 17:32:41.113 UTC";
    statuses.put(
        files.get(0),
        new StageFilesProcessor.IngestEntry(
            InternalUtils.IngestedFileStatus.NOT_FOUND,
            ZonedDateTime.parse(fileUploadTime, formatter).toInstant().toEpochMilli()));
    victim.updateFileStatus(statuses);

    assertThat(victim.query(filters.trackableFilesPredicate).collect(Collectors.toList()))
        .isNotEmpty();
    assertThat(victim.query(filters.matureFilePredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
    assertThat(victim.hasDirtyFiles()).isFalse();
  }
}
