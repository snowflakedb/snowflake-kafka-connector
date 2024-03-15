package com.snowflake.kafka.connector.internal;


import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * StageFileProcessor - class responsible for tracking files submitted to snowpipe for processing.
 * This class is responsible for:
 * - purging files with invalid offset - i.e. crash happened before offset was committed, but file was uploaded:
 *   it will be deleted from stage;
 * - purging files which have been successfully ingested - i.e. their load status is LOADED
 * - moving failed files (their status is FAILED or PARTIALLY_LOADED) or old files (1h) where no status is available,
 *   to the table stage.
 */
class StageFilesProcessor implements AutoCloseable {
    private final KCLogger LOGGER = new KCLogger(StageFilesProcessor.class.getName());
    private final String pipeName;
    private final String tableName;
    private final String stageName;
    private final String prefix;
    private final SnowflakeConnectionService conn;
    private final AtomicReference<Thread> runnerThread = new AtomicReference<>();
    private final TimeSupplier currentTimeSupplier;
    private final SnowflakeIngestionService ingestionService;
    private final SnowflakeTelemetryPipeStatus pipeTelemetry;
    private final SnowflakeTelemetryService telemetryService;
    private final ExecutorService cleanerExecutor;
    private final FilteringPredicates filters;

    /**
     * Client interface for the StageFileProcessor - allows thread safe registration of new files and notifies
     * the processor about offset update.
     */
    interface ProgressRegister {
        void registerNewStageFile(String fileName);
        void newOffset(long offset);
        SnowflakeTelemetryPipeCreation getPipeCreation();
        SnowflakeTelemetryPipeStatus getPipeTelemetry();
        SnowflakeTelemetryService getTelemetry();
        void close();
    }

    /**
     * This interface is a wrapper on System.currentTimeMillis() - required for testing purposes
     * */
    @FunctionalInterface
    interface TimeSupplier {
        long currentTime();
    }

    /**
     * This interface is a wrapper on Thread::sleep() - required for testing purposes
     */
    @FunctionalInterface
    interface TaskAwaiter {
        void await(long duration) throws InterruptedException;
    }

    public StageFilesProcessor(
            String pipeName,
            String tableName,
            String stageName,
            String prefix,
            SnowflakeConnectionService conn,
            SnowflakeIngestionService ingestionService,
            SnowflakeTelemetryPipeStatus pipeTelemetry,
            SnowflakeTelemetryService telemetryService)  {
        this(pipeName, tableName, stageName, prefix, conn, ingestionService, pipeTelemetry, telemetryService, System::currentTimeMillis);
    }

    @VisibleForTesting
    StageFilesProcessor(
            String pipeName,
            String tableName,
            String stageName,
            String prefix,
            SnowflakeConnectionService conn,
            SnowflakeIngestionService ingestionService,
            SnowflakeTelemetryPipeStatus pipeTelemetry,
            SnowflakeTelemetryService telemetryService,
            TimeSupplier currentTimeSupplier) {
        this.pipeName = pipeName;
        this.tableName = tableName;
        this.stageName = stageName;
        this.prefix = prefix;
        this.conn = conn;
        this.currentTimeSupplier = currentTimeSupplier;
        this.ingestionService = ingestionService;
        this.telemetryService = telemetryService;
        this.cleanerExecutor = ForkJoinPool.commonPool();
        this.pipeTelemetry = pipeTelemetry;
        this.filters = new FilteringPredicates(currentTimeSupplier);
    }

    /**
     * Starts stage files processor in the background. Uses one of the common thread pool's thread.
     * The processing keeps running until close() is invoked. Processing is executed every minute.
     * @return instance of ProgressRegister -
     *    client interface to interact with the worker thread in a thread safe manner.
     */
    public ProgressRegister trackFilesAsync() {
        SnowflakeTelemetryPipeCreation pipeCreation = new SnowflakeTelemetryPipeCreation(tableName, stageName, pipeName);
        ProgressRegisterImpl register = new ProgressRegisterImpl(pipeCreation, pipeTelemetry, telemetryService);
        register.owner = this;

        cleanerExecutor.submit(() -> trackFiles(
                register,
                Thread.currentThread(),
                () -> !Thread.currentThread().isInterrupted(),
                Thread::sleep));

        return register;
    }

    /**
     * trackFiles - actual processor logic with extracted thread management for testing purposes.
     * @param register - client interface
     * @param worker - normally - common pool worker thread doing the work; allows providing custom thread for testing
     * @param canContinue - delegate to Thread.currentThread().isInterrupted(), required for testing
     * @param awaiter - delegate to Thread.sleep(), required for testing
     */
    @VisibleForTesting
    void trackFiles(ProgressRegisterImpl register, Thread worker, Supplier<Boolean> canContinue, TaskAwaiter awaiter) {
        runnerThread.set(worker);
        boolean shouldFetchInitialStageFiles = true;
        boolean firstRun = true;

        List<String> files = new ArrayList<>();
        while (canContinue.get()) {
            try {
                register.getTelemetry().reportKafkaPartitionUsage(register.getPipeCreation(), false);

                // wait 1 minute
                awaiter.await(Duration.ofMinutes(1L).toMillis());
                // do we need to build some initial state based on the remote state?
                if (shouldFetchInitialStageFiles) {
                    files.addAll(fetchCurrentStage());
                    shouldFetchInitialStageFiles = false;
                }
                // add all files which might have been collected in the past minute while waiting for data
                register.files.drainTo(files);
                // process the files, store the spillover ones for the next cycle. in case of an error - we
                // will retry processing with the current file set in next iteration (with potentially newly added files)
                files = nextCheck(files, register, firstRun);
            } catch (InterruptedException e) {
                LOGGER.info("Cleaner terminated by an interrupt:\n{}", e.getMessage());
                worker.interrupt();
                return;
            } catch (Exception e) {
                register.getTelemetry().reportKafkaConnectFatalError(e.getMessage());
                LOGGER.warn(
                        "Cleaner encountered an exception {}:\n{}\n{}",
                        e.getClass(),
                        e.getMessage(),
                        e.getStackTrace());
                shouldFetchInitialStageFiles = true;
            }
            firstRun = false;
        }
    }

    @Override
    public void close() {
        Thread runner = runnerThread.getAndSet(null);
        if (runner != null) {
            runner.interrupt();
            try {
                runner.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private List<String> nextCheck(Collection<String> stageFiles, ProgressRegisterImpl register, boolean firstRun) {

        // make first categorization - split files into these with start offset higher than current
        FileCategorizer fileCategories = FileCategorizer.build(stageFiles, register.offset.get());

        if (firstRun) {
            register.getPipeCreation().setFileCountReprocessPurge(fileCategories.dirtyFiles.size());
            register.getPipeCreation().setFileCountRestart(fileCategories.stageFiles.size());
            register.getPipeTelemetry().addAndGetFileCountOnStage(fileCategories.stageFiles.size());
            register.getPipeTelemetry().addAndGetFileCountOnIngestion(fileCategories.stageFiles.size());
        }

        if (fileCategories.hasDirtyFiles()) {
            // we've found some files with offset "in future" - lets purge them, the data should be
            // reprocessed again and new file should be submitted for processing.
            purgeDirtyFiles(fileCategories.dirtyFiles);
        }

        if (fileCategories.hasStageFiles()) {
            // for all other files:
            // try to load ingest report and match the status to the file collection
            loadIngestReport(fileCategories);
            // if there are some stale files - i.e. older than 10 minutes, but no more than 1 hour - try
            // to fetch their history and update their state
            checkAndRefreshStaleFiles(fileCategories);
            // for all LOADED files - purge them from stage
            long now = currentTimeSupplier.currentTime();
            purgeLoadedFiles(fileCategories, (maxOffset, purgedFiles) -> {
                // update telemetry data after purge
                register.getPipeTelemetry().setPurgedOffsetAtomically(value -> Math.max(value, maxOffset));
                register.getPipeTelemetry().addAndGetFileCountOnStage(-purgedFiles);
                register.getPipeTelemetry().addAndGetFileCountOnIngestion(-purgedFiles);
                register.getPipeTelemetry().addAndGetFileCountPurged(purgedFiles);
            }, (purgedFile, ingestTime) -> {
                register.getPipeTelemetry().updateIngestionLag(now - ingestTime);
            });
            // for files with status { FAILED, PARTIALLY_LOADED } or older than 1 hour - move them to table stage
            moveFailedFiles(fileCategories, deletedFiles -> {
                register.getPipeTelemetry().addAndGetFileCountOnStage(-deletedFiles);
                register.getPipeTelemetry().addAndGetFileCountOnIngestion(-deletedFiles);
                register.getPipeTelemetry().updateFailedIngestionMetrics(deletedFiles);
            });
        }

        // any files we didn't process will spill over to the next clean cycle (neither purged nor moved to table stage)
        return fileCategories
                .query(filters.trackableFilesPredicate)
                .collect(Collectors.toList());
    }

    private void loadIngestReport(FileCategorizer fileCategories) {

        Map<String, InternalUtils.IngestedFileStatus> report = ingestionService
                .readIngestReport(new ArrayList<>(fileCategories.stageFiles.keySet()));
        fileCategories.updateFileStatus(report);
    }

    private void checkAndRefreshStaleFiles(FileCategorizer fileCategorizer) {
        long historyWindow = currentTimeSupplier.currentTime() - Duration.ofHours(1L).toMillis();
        List<String> staleFiles = fileCategorizer
                .query(filters.staledFilesPredicate)
                .collect(Collectors.toList());
        if (!staleFiles.isEmpty()) {
            LOGGER.debug("Checking stale file history for pipe: {}, staleFileCount: {}, staleFiles:{}",
                    pipeName,
                    staleFiles.size(),
                    String.join(", ", staleFiles));

            Map<String, InternalUtils.IngestedFileStatus> report =
                    ingestionService.readOneHourHistory(staleFiles, historyWindow);
            fileCategorizer.updateFileStatus(report);
        }
    }

    private void purgeLoadedFiles(FileCategorizer fileCategorizer, BiConsumer<Long, Integer> onPurgeFiles, BiConsumer<String, Long> onFilePurged) {
        AtomicLong maxFileOffset = new AtomicLong(Long.MIN_VALUE);
        List<String> loadedFiles = fileCategorizer
                .query(filters.loadedFilesPredicate)
                .peek(file -> {
                    long fileOffset = FileNameUtils.fileNameToEndOffset(file);
                    maxFileOffset.set(Math.max(fileOffset, maxFileOffset.get()));
                })
                .collect(Collectors.toList());

        if (!loadedFiles.isEmpty()) {
            LOGGER.debug("Purging loaded files for pipe: {}, loadedFileCount: {}, loadedFiles:{}",
                    pipeName,
                    loadedFiles.size(),
                    String.join(", ", loadedFiles));
            conn.purgeStage(stageName, loadedFiles);
            fileCategorizer.stopTrackingFiles(loadedFiles);

            onPurgeFiles.accept(maxFileOffset.get(), loadedFiles.size());
            loadedFiles.forEach(fileName ->
                    onFilePurged.accept(fileName, fileCategorizer.stageFiles.get(fileName).timestamp));
        }
    }

    private void moveFailedFiles(FileCategorizer fileCategorizer, Consumer<Integer> onMoveFiles) {
        List<String> failedFiles = fileCategorizer
                .query(filters.failedFilesPredicate)
                .collect(Collectors.toList());
        if (!failedFiles.isEmpty()) {
            LOGGER.debug(
                    "Moving failed files for pipe:{} to tableStage failedFileCount:{}, failedFiles:{}",
                    pipeName,
                    failedFiles.size(),
                    String.join(", ", failedFiles));
            conn.moveToTableStage(tableName, stageName, failedFiles);
            fileCategorizer.stopTrackingFiles(failedFiles);

            onMoveFiles.accept(failedFiles.size());
        }
    }

    private Collection<String> fetchCurrentStage() {
        try {
            return conn.listStage(stageName, prefix);
        } catch (Throwable t) {
            LOGGER.warn("Failed to fetch current stage state due to error:\n{}", t.getMessage());
            return new ArrayList<>();
        }
    }

    private void purgeDirtyFiles(Set<String> files) {
        try {
            LOGGER.info(
                    "Purging files already present on the stage for pipe{} before start. reprocessFileSize:{}, files: {}",
                    pipeName,
                    files.size(),
                    String.join(", ", files));
            conn.purgeStage(stageName, new ArrayList<>(files));
        } catch (Exception e) {
            LOGGER.error(
                    "Reprocess cleaner encountered an exception {}:\n{}\n{}",
                    e.getClass(),
                    e.getMessage(),
                    e.getStackTrace());
        }
    }

    public static class FileCategorizer {
        private final Set<String> dirtyFiles = new HashSet<>();
        private final Map<String, IngestEntry> stageFiles = new HashMap<>();
        private final long currentOffset;

        static FileCategorizer build(Collection<String> files, long currentOffset) {
            return files
                    .stream()
                    .reduce(
                        new FileCategorizer(currentOffset),
                        FileCategorizer::categorizeFile,
                        FileCategorizer::combine);
        }

        private FileCategorizer(long startOffset) {
            this.currentOffset = startOffset;
        }

        private static FileCategorizer combine(FileCategorizer lhs, FileCategorizer rhs) {
            FileCategorizer result = new FileCategorizer(lhs.currentOffset);
            result.stageFiles.putAll(lhs.stageFiles);
            result.stageFiles.putAll(rhs.stageFiles);
            result.dirtyFiles.addAll(lhs.dirtyFiles);
            result.dirtyFiles.addAll(rhs.dirtyFiles);
            return result;
        }

        private FileCategorizer categorizeFile(String file) {
            long fileOffset = FileNameUtils.fileNameToStartOffset(file);
            long timestamp = FileNameUtils.fileNameToTimeIngested(file);
            if (currentOffset <= fileOffset) {
                dirtyFiles.add(file);
            } else {
                IngestEntry entry = new IngestEntry(InternalUtils.IngestedFileStatus.NOT_FOUND, timestamp);
                stageFiles.put(file, entry);
            }
            return this;
        }

        void updateFileStatus(Map<String, InternalUtils.IngestedFileStatus> report) {
            report.forEach((fileName, status) -> stageFiles.computeIfPresent(fileName, (key, entry) -> {
                entry.status = status;
                return entry;
            }));
        }

        boolean hasDirtyFiles() {
            return !dirtyFiles.isEmpty();
        }

        boolean hasStageFiles() {
            return !stageFiles.isEmpty();
        }

        Stream<String> query(Predicate<Map.Entry<String, IngestEntry>> filter) {
            return stageFiles
                    .entrySet()
                    .stream()
                    .filter(filter)
                    .map(Map.Entry::getKey);
        }

        void stopTrackingFiles(List<String> files) {
            files.forEach(file -> stageFiles.computeIfPresent(file, (key, entry) -> {
                entry.keepTracking = false;
                return entry;
            }));
        }
    }

    @VisibleForTesting
    static class IngestEntry {
        private InternalUtils.IngestedFileStatus status;
        private final long timestamp;
        private boolean keepTracking = true;

        IngestEntry(InternalUtils.IngestedFileStatus status, long timestamp) {
            this.status = status;
            this.timestamp = timestamp;
        }
    }

    static class ProgressRegisterImpl implements ProgressRegister {
        private final LinkedBlockingQueue<String> files = new LinkedBlockingQueue<>();
        private final AtomicLong offset = new AtomicLong(Long.MAX_VALUE);
        private final SnowflakeTelemetryPipeCreation pipeCreation;
        private final SnowflakeTelemetryPipeStatus pipeTelemetry;
        private final  SnowflakeTelemetryService telemetryService;
        private StageFilesProcessor owner = null;

        ProgressRegisterImpl(SnowflakeTelemetryPipeCreation pipeCreation, SnowflakeTelemetryPipeStatus pipeTelemetry, SnowflakeTelemetryService telemetryService) {
            this.pipeCreation = pipeCreation;
            this.pipeTelemetry = pipeTelemetry;
            this.telemetryService = telemetryService;
        }

        @Override
        public void registerNewStageFile(String fileName) {
            files.add(fileName);
        }

        @Override
        public void newOffset(long offset) {
            this.offset.set(offset);
        }

        @Override
        public SnowflakeTelemetryPipeCreation getPipeCreation() {
            return pipeCreation;
        }

        @Override
        public SnowflakeTelemetryPipeStatus getPipeTelemetry() {
            return pipeTelemetry;
        }

        @Override
        public  SnowflakeTelemetryService getTelemetry() {
            return telemetryService;
        }

        @Override
        public void close() {
            if (owner != null) {
                owner.close();
                owner = null;
            }
        }
    }

    static class FilteringPredicates {
        @VisibleForTesting
        final Predicate <Map.Entry<String, IngestEntry>> loadedFilesPredicate;
        @VisibleForTesting
        final Predicate<Map.Entry<String, IngestEntry>> failedFilesPredicate;
        @VisibleForTesting
        final Predicate<Map.Entry<String, IngestEntry>> staledFilesPredicate;
        @VisibleForTesting
        final Predicate<Map.Entry<String, IngestEntry>> trackableFilesPredicate;

        public FilteringPredicates(TimeSupplier timeSupplier) {
            // after each cycle we can end up with a set of files we should keep tracking - file,
            // which was either purged or moved to table storage will have this flag cleared - we don't need to track it anymore
            trackableFilesPredicate = entry -> entry.getValue().keepTracking;

            // loaded files - simple case - their ingest status is LOADED
            loadedFilesPredicate = entry ->
                    trackableFilesPredicate.test(entry)
                    && entry.getValue().status == InternalUtils.IngestedFileStatus.LOADED;

            // failed files, either:
            failedFilesPredicate = entry ->
                    trackableFilesPredicate.test(entry)
                    // their status is partially loaded
                    && (entry.getValue().status == InternalUtils.IngestedFileStatus.PARTIALLY_LOADED
                    // or their status is failed
                    || entry.getValue().status == InternalUtils.IngestedFileStatus.FAILED
                    // or they are NOT loaded, but been sitting around for over one hour
                    || (entry.getValue().timestamp <= timeSupplier.currentTime() - Duration.ofHours(1).toMillis()
                    && loadedFilesPredicate.negate().test(entry)));

            // stale files - this is potentially grey zone - the files have not yet been processed, they are not marked as
            // failed yet, we don't know their status, but they are sitting in our register for over 10 minutes, but less than
            // one hour
            staledFilesPredicate = entry ->
                    trackableFilesPredicate.test(entry)
                    && failedFilesPredicate.negate().test(entry)
                    && loadedFilesPredicate.negate().test(entry)
                    && entry.getValue().timestamp <= timeSupplier.currentTime() - Duration.ofMinutes(10).toMillis();
        }
    }
}
