package com.snowflake.kafka.connector.internal;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.snowflake.ingest.connection.HistoryResponse;

/**
 * StageFileProcessor - class responsible for tracking files submitted to snowpipe for processing.
 * This class is responsible for: - purging files with invalid offset - i.e. crash happened before
 * offset was committed, but file was uploaded: it will be deleted from stage; - purging files which
 * have been successfully ingested - i.e. their load status is LOADED - moving failed files (their
 * status is FAILED or PARTIALLY_LOADED) or old files (1h) where no status is available, to the
 * table stage.
 */
class StageFilesProcessor {
  private static final KCLogger LOGGER = new KCLogger(StageFilesProcessor.class.getName());
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
   * Client interface for the StageFileProcessor - allows thread safe registration of new files and
   * notifies the processor about offset update.
   */
  interface ProgressRegister extends Closeable {
    void registerNewStageFile(String fileName);

    void newOffset(long offset);

    void close();
  }

  /** This interface is a wrapper on System.currentTimeMillis() - required for testing purposes */
  @FunctionalInterface
  interface TimeSupplier {
    long currentTime();
  }

  /** This interface is a wrapper on Thread::sleep() - required for testing purposes */
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
      SnowflakeTelemetryService telemetryService) {
    this(
        pipeName,
        tableName,
        stageName,
        prefix,
        conn,
        ingestionService,
        pipeTelemetry,
        telemetryService,
        System::currentTimeMillis);
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
    this.filters = new FilteringPredicates(currentTimeSupplier, prefix);
  }

  /**
   * Starts stage files processor in the background. Uses one of the common thread pool's thread.
   * The processing keeps running until close() is invoked. Processing is executed every minute.
   *
   * @return instance of ProgressRegister - client interface to interact with the worker thread in a
   *     thread safe manner.
   */
  public ProgressRegister trackFilesAsync() {
    SnowflakeTelemetryPipeCreation pipeCreation =
        new SnowflakeTelemetryPipeCreation(tableName, stageName, pipeName);
    ProgressRegisterImpl register = new ProgressRegisterImpl(this);

    ProgressRegistryTelemetry telemetry =
        new ProgressRegistryTelemetry(pipeCreation, pipeTelemetry, telemetryService);

    cleanerExecutor.submit(
        () ->
            trackFiles(
                register, telemetry, () -> !Thread.currentThread().isInterrupted(), Thread::sleep));

    register.waitForStart();
    return register;
  }

  /**
   * trackFiles - actual processor logic with extracted thread management for testing purposes.
   *
   * @param register - client interface
   * @param progressTelemetry - telemetry interface
   * @param canContinue - delegate to Thread.currentThread().isInterrupted(), required for testing
   * @param awaiter - delegate to Thread.sleep(), required for testing
   */
  @VisibleForTesting
  void trackFiles(
      ProgressRegisterImpl register,
      ProgressRegistryTelemetry progressTelemetry,
      Supplier<Boolean> canContinue,
      TaskAwaiter awaiter) {
    LOGGER.info(
        "Starting file cleaner for pipe {} on thread {}...",
        pipeName,
        Thread.currentThread().getName());
    runnerThread.set(Thread.currentThread());
    // all is configured, let the caller resume operation
    register.markStarted();
    boolean shouldFetchInitialStageFiles = true;
    boolean firstRun = true;

    ProcessorContext ctx =
        new ProcessorContext(progressTelemetry, currentTimeSupplier.currentTime());

    while (canContinue.get()) {
      try {
        // update metrics
        progressTelemetry.reportKafkaPartitionUsage(false);

        // add all files which might have been collected during the last cycle for processing
        register.transferFilesToContext(ctx);

        // wait 1 minute
        awaiter.await(Duration.ofMinutes(1L).toMillis());

        // do we need to build some initial state based on the remote state?
        if (shouldFetchInitialStageFiles) {
          shouldFetchInitialStageFiles = false;
          initializeCleanStartState(ctx);
        }

        LOGGER.debug(
            "starting cleanup for pipe {} with {} files and history with {} entries",
            pipeName,
            ctx.files.size(),
            ctx.ingestHistory.size());

        // process the files, store the spillover ones for the next cycle. in case of an error - we
        // will retry processing with the current file set in next iteration (with potentially newly
        // added files)
        nextCheck(ctx, register, firstRun);

      } catch (InterruptedException e) {
        LOGGER.info("Cleaner terminated by an interrupt:\n{}", e.getMessage());
        break;
      } catch (Exception e) {
        progressTelemetry.reportKafkaConnectFatalError(e.getMessage());
        LOGGER.warn(
            "Cleaner encountered an exception {}:\n{}\n{}",
            e.getClass(),
            e.getMessage(),
            e.getStackTrace());

        shouldFetchInitialStageFiles = true;
        // as the next cycle will load files from remote due to an error, we can reset tracking
        // history timestamp to now
        // (all older entries would be tracked by stale or old files anyway)
        ctx.startTrackingHistoryTimestamp = currentTimeSupplier.currentTime();
      }
      firstRun = false;
    }
    runnerThread.set(null);
    LOGGER.info(
        "Stopping file cleaner for pipe {} - thread {} will return to the pool, wasInterrupted: {}",
        pipeName,
        Thread.currentThread().getName(),
        Thread.interrupted());
    // all is completed, let the caller resume processing
    register.markCompleted();
  }

  private void close() {
    Thread runner = runnerThread.getAndSet(null);
    if (runner != null) {
      runner.interrupt();
    }
  }

  private void initializeCleanStartState(ProcessorContext ctx) {
    ctx.files.addAll(fetchCurrentStage());
    // since we will load completely fresh history from remote, we can reset the history tracking
    // state
    ctx.ingestHistory.clear();
    ctx.historyMarker.set(null);
  }

  private void nextCheck(ProcessorContext ctx, ProgressRegisterImpl register, boolean firstRun) {

    // make first categorization - split files into these with start offset higher than current
    FileCategorizer fileCategories = FileCategorizer.build(ctx.files, register.offset.get());

    if (firstRun) {
      ctx.progressTelemetry.initializeStats(
          fileCategories.dirtyFiles.size(), fileCategories.stageFiles.size());
    }

    if (fileCategories.hasDirtyFiles()) {
      // we've found some files with offset "in future" - lets purge them, the data should be
      // reprocessed again and new file should be submitted for processing.
      purgeDirtyFiles(fileCategories.dirtyFiles);
    }

    if (fileCategories.hasStageFiles()) {
      // for all other files:
      // try to load ingest report and match the status to the file collection
      loadIngestReport(fileCategories, ctx);
      // if there are some stale files - i.e. older than 10 minutes, but no more than 1 hour - try
      // to fetch their history and update their state
      checkAndRefreshStaleFiles(fileCategories);
      // for all LOADED files - purge them from stage
      purgeLoadedFiles(
          fileCategories,
          ctx,
          ctx.progressTelemetry::notifyFilesPurged,
          ctx.progressTelemetry::notifyFileIngestLag);
      // for files with status { FAILED, PARTIALLY_LOADED } or older than 1 hour - move them to
      // table stage
      moveFailedFiles(fileCategories, ctx, ctx.progressTelemetry::notifyFilesDeleted);
    }

    // any files we didn't process will spill over to the next clean cycle (neither purged nor moved
    // to table stage)
    List<String> filesToTrack =
        fileCategories.query(filters.trackableFilesPredicate).collect(Collectors.toList());
    LOGGER.debug(
        "keep {} files and {} history entries for next cycle for pipe {}",
        filesToTrack.size(),
        ctx.ingestHistory.size(),
        pipeName);
    ctx.files.clear();
    ctx.files.addAll(filesToTrack);
  }

  private void loadIngestReport(FileCategorizer fileCategories, ProcessorContext ctx) {
    // do not look up history older than necessary - as this is not absolute time, add 5 seconds
    // overlap to be sure we accommodate for transmission delay
    long secondsSinceStart =
        Duration.ofMillis(currentTimeSupplier.currentTime() - ctx.startTrackingHistoryTimestamp)
                .getSeconds()
            + 5;

    ingestionService.readIngestHistoryForward(
        ctx.ingestHistory,
        filters.currentPartitionFilePredicate,
        ctx.historyMarker,
        (int) secondsSinceStart);
    fileCategories.updateFileStatus(ctx.ingestHistory);
  }

  private void checkAndRefreshStaleFiles(FileCategorizer fileCategorizer) {
    long historyWindow = currentTimeSupplier.currentTime() - Duration.ofHours(1L).toMillis();
    List<String> staleFiles =
        fileCategorizer.query(filters.staledFilesPredicate).collect(Collectors.toList());
    if (!staleFiles.isEmpty()) {
      LOGGER.debug(
          "Checking stale file history for pipe: {}, staleFileCount: {}, staleFiles:{}",
          pipeName,
          staleFiles.size(),
          String.join(", ", staleFiles));

      Map<String, InternalUtils.IngestedFileStatus> report =
          ingestionService.readOneHourHistory(staleFiles, historyWindow);
      fileCategorizer.updateFileStatus(report);
    }
  }

  private void purgeLoadedFiles(
      FileCategorizer fileCategorizer,
      ProcessorContext ctx,
      BiConsumer<Long, Integer> onPurgeFiles,
      BiConsumer<String, Long> onFilePurged) {
    AtomicLong maxFileOffset = new AtomicLong(Long.MIN_VALUE);
    List<String> loadedFiles =
        fileCategorizer
            .query(filters.loadedFilesPredicate)
            .peek(
                file -> {
                  long fileOffset = FileNameUtils.fileNameToEndOffset(file);
                  maxFileOffset.set(Math.max(fileOffset, maxFileOffset.get()));
                })
            .collect(Collectors.toList());

    if (!loadedFiles.isEmpty()) {
      LOGGER.debug(
          "Purging loaded files for pipe: {}, loadedFileCount: {}, loadedFiles:{}",
          pipeName,
          loadedFiles.size(),
          String.join(", ", loadedFiles));
      conn.purgeStage(stageName, loadedFiles);
      stopTrackingFiles(loadedFiles, fileCategorizer, ctx);

      onPurgeFiles.accept(maxFileOffset.get(), loadedFiles.size());
      loadedFiles.forEach(
          fileName ->
              onFilePurged.accept(fileName, fileCategorizer.stageFiles.get(fileName).timestamp));
    }
  }

  private void moveFailedFiles(
      FileCategorizer fileCategorizer, ProcessorContext ctx, Consumer<Integer> onMoveFiles) {
    List<String> failedFiles =
        fileCategorizer.query(filters.failedFilesPredicate).collect(Collectors.toList());
    if (!failedFiles.isEmpty()) {
      LOGGER.debug(
          "Moving failed files for pipe:{} to tableStage failedFileCount:{}, failedFiles:{}",
          pipeName,
          failedFiles.size(),
          String.join(", ", failedFiles));
      conn.moveToTableStage(tableName, stageName, failedFiles);
      stopTrackingFiles(failedFiles, fileCategorizer, ctx);
      onMoveFiles.accept(failedFiles.size());
    }
  }

  private void stopTrackingFiles(
      List<String> files, FileCategorizer fileCategorizer, ProcessorContext ctx) {
    fileCategorizer.stopTrackingFiles(files);
    files.forEach(ctx.ingestHistory::remove);
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
          "Purging files already present on the stage for pipe{} before start."
              + " reprocessFileSize:{}, files: {}",
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
      FileCategorizer categorizer = new FileCategorizer(currentOffset);
      files.forEach(categorizer::categorizeFile);
      return categorizer;
    }

    private FileCategorizer(long startOffset) {
      this.currentOffset = startOffset;
    }

    private void categorizeFile(String file) {
      long fileOffset = FileNameUtils.fileNameToStartOffset(file);
      long timestamp = FileNameUtils.fileNameToTimeIngested(file);
      if (currentOffset <= fileOffset) {
        dirtyFiles.add(file);
      } else {
        IngestEntry entry = new IngestEntry(InternalUtils.IngestedFileStatus.NOT_FOUND, timestamp);
        stageFiles.put(file, entry);
      }
    }

    void updateFileStatus(Map<String, InternalUtils.IngestedFileStatus> report) {
      report.forEach(
          (fileName, status) ->
              stageFiles.computeIfPresent(
                  fileName,
                  (key, entry) -> {
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
      return stageFiles.entrySet().stream().filter(filter).map(Map.Entry::getKey);
    }

    void stopTrackingFiles(List<String> files) {
      files.forEach(
          file ->
              stageFiles.computeIfPresent(
                  file,
                  (key, entry) -> {
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
    private static final KCLogger LOGGER = new KCLogger(ProgressRegister.class.getName());
    private final LinkedBlockingQueue<String> files = new LinkedBlockingQueue<>();
    private final AtomicLong offset = new AtomicLong(Long.MAX_VALUE);
    private final CountDownLatch startSignal = new CountDownLatch(1);
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final AtomicReference<StageFilesProcessor> owner = new AtomicReference<>();

    public ProgressRegisterImpl(StageFilesProcessor filesProcessor) {
      owner.set(filesProcessor);
    }

    @Override
    public void registerNewStageFile(String fileName) {
      LOGGER.debug("Start tracking new file {}", fileName);
      files.add(fileName);
    }

    @Override
    public void newOffset(long offset) {
      LOGGER.trace("New offset: {}", offset);
      this.offset.set(offset);
    }

    @Override
    public void close() {
      StageFilesProcessor processor = owner.getAndSet(null);
      if (processor != null) {
        LOGGER.info("Client closed");
        processor.close();
        try {
          stopSignal.await(30L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOGGER.debug("interrupt while waiting for close");
        }
      }
    }

    private void transferFilesToContext(ProcessorContext ctx) {
      List<String> freshFiles = new ArrayList<>();
      files.drainTo(freshFiles);
      StageFilesProcessor processor = owner.get();
      LOGGER.debug(
          "collected {} files for processing for pipe {}",
          freshFiles.size(),
          processor == null ? "n/a" : processor.pipeName);
      ctx.files.addAll(freshFiles);
    }

    private void markStarted() {
      startSignal.countDown();
    }

    private void markCompleted() {
      stopSignal.countDown();
    }

    @VisibleForTesting
    void waitForStart() {
      boolean started = false;
      try {
        started = startSignal.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOGGER.debug("interrupt while waiting for cleaner to start");
      }
      if (!started) {
        throw SnowflakeErrors.ERROR_5024.getException();
      }
    }
  }

  static class FilteringPredicates {
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> loadedFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> failedFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> staledFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> trackableFilesPredicate;
    @VisibleForTesting final Predicate<HistoryResponse.FileEntry> currentPartitionFilePredicate;

    public FilteringPredicates(TimeSupplier timeSupplier, String filePrefix) {
      // after each cycle we can end up with a set of files we should keep tracking - file,
      // which was either purged or moved to table storage will have this flag cleared - we don't
      // need to track it anymore
      trackableFilesPredicate = entry -> entry.getValue().keepTracking;

      // loaded files - simple case - their ingest status is LOADED
      loadedFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  && entry.getValue().status == InternalUtils.IngestedFileStatus.LOADED;

      // failed files, either:
      failedFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  // their status is partially loaded
                  && (entry.getValue().status == InternalUtils.IngestedFileStatus.PARTIALLY_LOADED
                      // or their status is failed
                      || entry.getValue().status == InternalUtils.IngestedFileStatus.FAILED
                      // or they are NOT loaded, but been sitting around for over one hour
                      || (entry.getValue().timestamp
                              <= timeSupplier.currentTime() - Duration.ofHours(1).toMillis()
                          && loadedFilesPredicate.negate().test(entry)));

      // stale files - this is potentially grey zone - the files have not yet been processed, they
      // are not marked as
      // failed yet, we don't know their status, but they are sitting in our register for over 10
      // minutes, but less than
      // one hour
      staledFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  && failedFilesPredicate.negate().test(entry)
                  && loadedFilesPredicate.negate().test(entry)
                  && entry.getValue().timestamp
                      <= timeSupplier.currentTime() - Duration.ofMinutes(10).toMillis();

      // stage files filter - history report returns all entries for given stage across all
      // partitions
      // but individual processor instance is interested only in tracking files for "this"
      // partition,
      // thus the file filter - pick up history entries only for files in given partition
      String prefix = filePrefix.toUpperCase();
      currentPartitionFilePredicate =
          fileEntry -> fileEntry.getPath().toUpperCase().startsWith(prefix);
    }
  }

  // data class, to keep the execution context in a single place, rather than pass it around via
  // parameters
  private static class ProcessorContext {
    final List<String> files = new ArrayList<>();
    final Map<String, InternalUtils.IngestedFileStatus> ingestHistory = new HashMap<>();
    final AtomicReference<String> historyMarker = new AtomicReference<>();
    final ProgressRegistryTelemetry progressTelemetry;
    long startTrackingHistoryTimestamp;

    private ProcessorContext(
        ProgressRegistryTelemetry progressTelemetry, long startTrackingHistoryTimestamp) {
      this.progressTelemetry = progressTelemetry;
      this.startTrackingHistoryTimestamp = startTrackingHistoryTimestamp;
    }
  }
}
