package com.snowflake.kafka.connector.internal;

import com.google.common.base.Functions;
import net.snowflake.client.jdbc.internal.joda.time.DateTime;
import net.snowflake.client.jdbc.internal.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class StageFilesProcessor_FileCategorizerTest {
    @Test
    void allFilesWillBeCategorizedAsStageFilesWhenNoInitialOffsetIsSupplied() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofSeconds(i).toMillis() )
        ).collect(Collectors.toList());

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);

        assertThat(victim.hasDirtyFiles()).isFalse();
        assertThat(victim.hasStageFiles()).isTrue();
        assertThat(victim.query(t -> true).collect(Collectors.toList())).containsAll(files);
    }

    @Test
    void allFilesWillBeCategorizedAsDirtyFilesIfOffsetIsSmallerThanLastSubmittedFile() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofSeconds(i).toMillis() )
                ).collect(Collectors.toList());

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, 0);

        assertThat(victim.hasDirtyFiles()).isTrue();
        assertThat(victim.hasStageFiles()).isFalse();
        assertThat(victim.query(t -> true).collect(Collectors.toList())).isEmpty();
    }

    @Test
    void willCategorizeAllFilesAsLoadedWhenMatchingStatusIsProvided() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        StageFilesProcessor.TimeSupplier timeSupplier = () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
        StageFilesProcessor.FilteringPredicates filters = new StageFilesProcessor.FilteringPredicates(timeSupplier);

        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofMinutes(i).toMillis() )
                ).collect(Collectors.toList());
        Map<String, InternalUtils.IngestedFileStatus> statuses = files
                .stream()
                .collect(Collectors.toMap(Functions.identity(), fileName -> InternalUtils.IngestedFileStatus.LOADED));

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);
        victim.updateFileStatus(statuses);

        assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).containsAll(files);
        assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
    }

    @Test
    void willCategorizeAllFilesAsFailedWhenMatchingStatusIsProvided() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        StageFilesProcessor.TimeSupplier timeSupplier = () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
        StageFilesProcessor.FilteringPredicates filters = new StageFilesProcessor.FilteringPredicates(timeSupplier);

        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofMinutes(i).toMillis() )
                ).collect(Collectors.toList());

        InternalUtils.IngestedFileStatus[] failedStatus = new InternalUtils.IngestedFileStatus[] {
                InternalUtils.IngestedFileStatus.FAILED, InternalUtils.IngestedFileStatus.PARTIALLY_LOADED
        };
        AtomicInteger idx = new AtomicInteger(0);

        Map<String, InternalUtils.IngestedFileStatus> statuses = files
                .stream()
                .collect(Collectors.toMap(Functions.identity(), fileName -> failedStatus[idx.getAndIncrement() % failedStatus.length]));

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);
        victim.updateFileStatus(statuses);

        assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).containsAll(files);
        assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
    }

    @Test
    void willCategorizeAllFilesAsFailedWhenNoStatusIsProvidedButAllFilesAreOlderThanOneHour() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        StageFilesProcessor.TimeSupplier timeSupplier = () -> ts.getMillis() + Duration.ofMinutes(101L).toMillis();
        StageFilesProcessor.FilteringPredicates filters = new StageFilesProcessor.FilteringPredicates(timeSupplier);

        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofMinutes(i).toMillis() )
                ).collect(Collectors.toList());

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);

        assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).containsAll(files);
        assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();
    }

    @Test
    void willCategorizeAllFilesAsStaleWhenNoStatusIsProvidedButAllFilesAreOlderThanMinutesButNotOlderThanOneHour() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        StageFilesProcessor.TimeSupplier timeSupplier = () -> ts.getMillis() + Duration.ofMinutes(45L).toMillis();
        StageFilesProcessor.FilteringPredicates filters = new StageFilesProcessor.FilteringPredicates(timeSupplier);

        List<String> files = IntStream
                .rangeClosed(0, 9)
                .mapToObj(i -> String.format("connector/topic/0/%d_%d_%d.json.gz", i * 100, ((i+1)*100)-1, ts.getMillis() + Duration.ofMinutes(10L + i).toMillis() )
                ).collect(Collectors.toList());

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);

        assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).containsAll(files);;
    }

    @Test
    void onlyFilesMarkedAsTrackableWillBeProvided() {
        DateTime ts = new DateTime(2000, 1, 10, 12, 0, DateTimeZone.UTC);
        StageFilesProcessor.TimeSupplier timeSupplier = () -> ts.getMillis() + Duration.ofMinutes(61L).toMillis();
        StageFilesProcessor.FilteringPredicates filters = new StageFilesProcessor.FilteringPredicates(timeSupplier);

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
        files.add(0, String.format("connector/topic/0/30_39_%d.json.gz", ts.getMillis() + Duration.ofMinutes(40).toMillis()));
        // fresh file, no status
        files.add(0, String.format("connector/topic/0/40_49_%d.json.gz", ts.getMillis() + Duration.ofMinutes(60).toMillis()));
        // fresh file, failed
        files.add(0, String.format("connector/topic/0/50_59_%d.json.gz", ts.getMillis() + Duration.ofMinutes(60).toMillis()));
        statuses.put(files.get(0), InternalUtils.IngestedFileStatus.PARTIALLY_LOADED);
        // fresh file, loaded
        files.add(0, String.format("connector/topic/0/60_69_%d.json.gz", ts.getMillis() + Duration.ofMinutes(60).toMillis()));
        statuses.put(files.get(0), InternalUtils.IngestedFileStatus.LOADED);

        StageFilesProcessor.FileCategorizer victim = StageFilesProcessor.FileCategorizer.build(files, Long.MAX_VALUE);
        victim.stopTrackingFiles(files);

        assertThat(victim.query(filters.loadedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.failedFilesPredicate).collect(Collectors.toList())).isEmpty();
        assertThat(victim.query(filters.staledFilesPredicate).collect(Collectors.toList())).isEmpty();;
    }
}