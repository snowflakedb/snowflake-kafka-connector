package com.snowflake.kafka.connector.internal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeCreation;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryPipeStatus;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 * This class is operating as follows:
 * <li>fetch initial state of the stage by listing the stage's content
 * <li>via the ProgressRegister interface it keeps track of the ingested files and most recently
 *     committed offset Processing is running in a loop - 1 execution per minute, for every topic's
 *     partition. The algorithm is following:
 * <li>select all data files submitted for ingestion and uploaded to stage
 * <li>wait one minute for the files to be processed, new files can be added, but will be processed
 *     in next cycle
 * <li>for the first execution / or whenever there is an error - fetch state of the remote stage
 * <li>merge content of the remote stage with the recently uploaded files and with files spilled
 *     from previous cycle
 * <li>categorize the files - if there are files with start offset greater than most recently
 *     submitted offset it means a restart/rebalance/crash - content of such file would be generated
 *     again, so it is safe to delete it; consider all other files as active stage files
 * <li>delete dirty files (if any)
 * <li>for stage files - load ingest history and mark the file as loaded, failed (failed or
 *     partially loaded) or unchanged
 * <li>for all files which do not have a status - if they are older than 1 hour - mark them as
 *     FAILED. files aged 10-60 are considered to be stale - we keep an eye on them for any status
 *     change, until one is found or they grow older than 1 hour
 * <li>for all files marked as LOADED - delete them from stage
 * <li>for all files marked as FAILED - move them to table stage
 * <li>for all remaining files, let them spill over to the next clean cycle
 */
class StageFilesProcessor {
  private static final KCLogger LOGGER = new KCLogger(StageFilesProcessor.class.getName());
  private final String pipeName;
  private final String tableName;
  private final String stageName;
  private final String prefix;
  private final String topic;
  private final int partition;
  private final SnowflakeConnectionService conn;
  private final AtomicReference<ScheduledFuture<?>> cleanerTaskHolder = new AtomicReference<>();
  private final TimeSupplier currentTimeSupplier;
  private final SnowflakeIngestionService ingestionService;
  private final SnowflakeTelemetryPipeStatus pipeTelemetry;
  private final SnowflakeTelemetryService telemetryService;
  private final FilteringPredicates filters;
  private final ScheduledExecutorService schedulingExecutor;
  private final long v2CleanerIntervalSeconds;
  // start first cleanup cycle 60 seconds after start
  private static final long INITIAL_DELAY_SECONDS = 60;

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

  public StageFilesProcessor(
      String pipeName,
      String tableName,
      String stageName,
      String prefix,
      String topic,
      int partition,
      SnowflakeConnectionService conn,
      SnowflakeIngestionService ingestionService,
      SnowflakeTelemetryPipeStatus pipeTelemetry,
      SnowflakeTelemetryService telemetryService,
      ScheduledExecutorService schedulingExecutor,
      long v2CleanerIntervalSeconds) {
    this(
        pipeName,
        tableName,
        stageName,
        prefix,
        topic,
        partition,
        conn,
        ingestionService,
        pipeTelemetry,
        telemetryService,
        schedulingExecutor,
        System::currentTimeMillis,
        v2CleanerIntervalSeconds);
  }

  @VisibleForTesting
  StageFilesProcessor(
      String pipeName,
      String tableName,
      String stageName,
      String prefix,
      String topic,
      int partition,
      SnowflakeConnectionService conn,
      SnowflakeIngestionService ingestionService,
      SnowflakeTelemetryPipeStatus pipeTelemetry,
      SnowflakeTelemetryService telemetryService,
      ScheduledExecutorService schedulingExecutor,
      TimeSupplier currentTimeSupplier,
      long v2CleanerIntervalSeconds) {
    this.pipeName = pipeName;
    this.tableName = tableName;
    this.stageName = stageName;
    this.prefix = prefix;
    this.topic = topic;
    this.partition = partition;
    this.conn = conn;
    this.currentTimeSupplier = currentTimeSupplier;
    this.ingestionService = ingestionService;
    this.telemetryService = telemetryService;
    this.pipeTelemetry = pipeTelemetry;
    this.schedulingExecutor = schedulingExecutor;
    this.filters = new FilteringPredicates(currentTimeSupplier, prefix);
    this.v2CleanerIntervalSeconds = v2CleanerIntervalSeconds;
  }

  /**
   * Starts stage files processor in the background. Uses one of the common thread pool's thread.
   * The processing keeps running until close() is invoked. Processing is executed every minute.
   *
   * @return instance of ProgressRegister - client interface to interact with the worker thread in a
   *     thread safe manner.
   */
  public ProgressRegister trackFilesAsync() {
    // if something was running - close it!
    close();

    SnowflakeTelemetryPipeCreation pipeCreation =
        preparePipeStartTelemetryEvent(tableName, stageName, pipeName);
    ProgressRegisterImpl register = new ProgressRegisterImpl(this);

    PipeProgressRegistryTelemetry telemetry =
        new PipeProgressRegistryTelemetry(pipeCreation, pipeTelemetry, telemetryService);

    trackFiles(register, telemetry);
    return register;
  }

  /**
   * trackFiles - actual processor logic with extracted thread management for testing purposes.
   *
   * @param register - client interface
   * @param progressTelemetry - telemetry interface
   */
  @VisibleForTesting
  void trackFiles(ProgressRegisterImpl register, PipeProgressRegistryTelemetry progressTelemetry) {
    LOGGER.info(
        "Starting file cleaner for pipe {} ... [matching stage files with prefix: {}]",
        pipeName,
        prefix);

    AtomicBoolean shouldFetchInitialStageFiles = new AtomicBoolean(true);
    AtomicBoolean isFirstRun = new AtomicBoolean(true);
    AtomicBoolean hadError = new AtomicBoolean(false);

    ProcessorContext ctx =
        new ProcessorContext(progressTelemetry, currentTimeSupplier.currentTime());
    // required for testing purposes
    register.currentProcessorContext = ctx;

    final String threadName =
        String.format("file-processor-[%s/%d:%s]", topic, partition, tableName);

    progressTelemetry.reportKafkaPartitionStart();

    cleanerTaskHolder.set(
        schedulingExecutor.scheduleWithFixedDelay(
            () -> {
              Thread.currentThread().setName(threadName);
              try {
                // cleaner starts along with the partition task, but until table, stage and pipe
                // aren't created - there is no point in querying the stage.
                if (isFirstRun.get()
                    && checkPreRequisites() != CleanerPrerequisites.PIPE_COMPATIBLE) {
                  LOGGER.debug(
                      "neither table {} nor stage {} nor pipe {} have been initialized yet,"
                          + " skipping cycle...",
                      tableName,
                      stageName,
                      pipeName);
                  return;
                }

                progressTelemetry.reportKafkaPartitionUsage();

                // add all files which might have been collected during the last cycle for
                // processing
                register.transferFilesToContext(ctx);

                // initialize state based on the remote stage state (do it on first call or after
                // error)
                if (shouldFetchInitialStageFiles.getAndSet(false)) {
                  initializeCleanStartState(ctx, isFirstRun.get());
                  isFirstRun.set(false);
                }

                LOGGER.debug(
                    "cleanup cycle {} for pipe {} with {} files and history with {} entries",
                    ctx.cleanupCycle.incrementAndGet(),
                    pipeName,
                    ctx.files.size(),
                    ctx.ingestHistory.size());

                // process the files, store the spillover ones for the next cycle. in case of an
                // error - we
                // will retry processing with the current file set in next iteration (with
                // potentially newly
                // added files)
                nextCheck(ctx, register, hadError.getAndSet(false));
              } catch (Exception e) {
                progressTelemetry.reportKafkaConnectFatalError(e.getMessage());
                LOGGER.warn(
                    "Cleaner encountered an exception {} in cycle {}:\n{}\n{}",
                    e.getClass(),
                    ctx.cleanupCycle.get(),
                    e.getMessage(),
                    e.getStackTrace());

                shouldFetchInitialStageFiles.set(true);
                hadError.set(true);
                // as the next cycle will load files from remote due to an error, we can reset
                // tracking
                // history timestamp to now
                // (all older entries would be tracked by stale or old files anyway)
                ctx.startTrackingHistoryTimestamp = currentTimeSupplier.currentTime();
              }
            },
            INITIAL_DELAY_SECONDS,
            v2CleanerIntervalSeconds,
            TimeUnit.SECONDS));
  }

  private void close() {
    ScheduledFuture<?> task = cleanerTaskHolder.getAndSet(null);
    if (task != null) {
      task.cancel(true);
    }
  }

  private void initializeCleanStartState(ProcessorContext ctx, boolean firstRun) {
    Collection<String> remoteStageFiles = fetchCurrentStage();
    if (firstRun) {
      HashSet<String> remoteFiles = new HashSet<>(remoteStageFiles);
      long remoteFileCount =
          ctx.files.stream().filter(localFile -> !remoteFiles.contains(localFile)).count();
      ctx.progressTelemetry.setupInitialState(remoteFileCount);
    }
    ctx.files.addAll(remoteStageFiles);
    // since we will load completely fresh history from remote, we can reset the history tracking
    // state
    ctx.ingestHistory.clear();
    ctx.historyMarker.set(null);
    LOGGER.debug("for pipe {} found {} file(s) on remote stage", pipeName, remoteStageFiles.size());
  }

  private void nextCheck(ProcessorContext ctx, ProgressRegisterImpl register, boolean hadErrors) {

    // make first categorization - split files into these with start offset higher than current
    FileCategorizer fileCategories =
        FileCategorizer.build(ctx.files, register.offset.get(), filters);

    if (hadErrors) {
      ctx.progressTelemetry.updateStatsAfterError(
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
      checkAndRefreshStaleFiles(fileCategories, ctx);
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
    cleanOldHistory(ctx);
    LOGGER.debug(
        "keep {} files and {} history entries for next cycle for pipe {}",
        filesToTrack.size(),
        ctx.ingestHistory.size(),
        pipeName);
    ctx.files.clear();
    ctx.files.addAll(filesToTrack);
    ctx.files.addAll(fileCategories.dirtyFiles);
  }

  private void loadIngestReport(FileCategorizer fileCategories, ProcessorContext ctx) {
    // do not look up history older than necessary - as this is not absolute time, add 5 seconds
    // overlap to be sure we accommodate for transmission delay
    long secondsSinceStart =
        Duration.ofMillis(currentTimeSupplier.currentTime() - ctx.startTrackingHistoryTimestamp)
                .getSeconds()
            + 5;

    Map<String, InternalUtils.IngestedFileStatus> history = new HashMap<>();
    ingestionService.readIngestHistoryForward(
        history, filters.currentPartitionFilePredicate, ctx.historyMarker, (int) secondsSinceStart);

    mergeHistory(ctx.ingestHistory, history);

    fileCategories.updateFileStatus(ctx.ingestHistory);
  }

  private void mergeHistory(
      Map<String, IngestEntry> trackedHistory,
      Map<String, InternalUtils.IngestedFileStatus> freshHistory) {
    long now = currentTimeSupplier.currentTime();
    // copy all new entries to the tracked history - update timestamp of history entry
    freshHistory.forEach(
        (file, status) ->
            trackedHistory.compute(file, (key, entry) -> new IngestEntry(status, now)));
  }

  private void checkAndRefreshStaleFiles(FileCategorizer fileCategorizer, ProcessorContext ctx) {
    long historyWindow = (currentTimeSupplier.currentTime() - Duration.ofHours(1L).toMillis());
    List<String> staleFiles =
        fileCategorizer.query(filters.staledFilesPredicate).collect(Collectors.toList());
    if (!staleFiles.isEmpty()) {
      // TODO: readOneHourHistory call is very heavy and may be throttled at our API side
      // consider changing the logic to read that history once every 10-15 minutes (would require
      // new or modified
      // readOneHourHistory method though)
      LOGGER.debug(
          "Checking stale file history for pipe: {}, staleFileCount: {}, staleFiles:{}",
          pipeName,
          staleFiles.size(),
          String.join(", ", staleFiles));

      Map<String, InternalUtils.IngestedFileStatus> report =
          ingestionService.readOneHourHistory(staleFiles, historyWindow);
      mergeHistory(ctx.ingestHistory, report);

      fileCategorizer.updateFileStatus(ctx.ingestHistory);
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
      // underlying code moves files one by one, so this is not going to impact performance.
      // we have been observing scenarios, when the file listed at the start of the process was
      // removed, or process didn't have access to it - this would cause cleaner to fail - so
      // instead we process file one by one to
      // ensure all the ones which are still present will be actually moved
      failedFiles.stream()
          .map(Lists::newArrayList)
          .forEach(
              failedFile -> {
                try {
                  conn.moveToTableStage(tableName, stageName, failedFile);
                } catch (SnowflakeKafkaConnectorException e) {
                  telemetryService.reportKafkaConnectFatalError(
                      String.format("[cleaner for pipe %s]: %s", pipeName, e.getMessage()));
                  LOGGER.warn(
                      "Could not move file {} for pipe {} to table stage due to {} <{}>\n"
                          + "File won't be tracked.",
                      failedFile.get(0),
                      pipeName,
                      e.getMessage(),
                      e.getClass().getName());
                }
              });
      stopTrackingFiles(failedFiles, fileCategorizer, ctx);
      onMoveFiles.accept(failedFiles.size());
    }
  }

  private void stopTrackingFiles(
      List<String> files, FileCategorizer fileCategorizer, ProcessorContext ctx) {
    fileCategorizer.stopTrackingFiles(files);
    files.forEach(ctx.ingestHistory::remove);
  }

  private void cleanOldHistory(ProcessorContext ctx) {
    int oldEntries = 0;
    for (Iterator<Map.Entry<String, IngestEntry>> it = ctx.ingestHistory.entrySet().iterator();
        it.hasNext(); ) {
      if (filters.oneHourOldEntryPredicate.test(it.next())) {
        it.remove();
        ++oldEntries;
      }
    }
    if (oldEntries > 0) {
      LOGGER.debug("for pipe {} removed {} old history entries", pipeName, oldEntries);
    }
  }

  private Collection<String> fetchCurrentStage() {
    try {
      List<String> stageFiles = conn.listStage(stageName, prefix);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Fetched for pipe: {} from stage: {} following files: {}",
            pipeName,
            stageName,
            String.join(", ", stageFiles));
      }
      return stageFiles;
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
      files.clear();
    } catch (Exception e) {
      LOGGER.error(
          "Reprocess cleaner encountered an exception {}:\n{}\n{}",
          e.getClass(),
          e.getMessage(),
          e.getStackTrace());
    }
  }

  enum CleanerPrerequisites {
    NONE,
    TABLE_COMPATIBLE,
    STAGE_COMPATIBLE,
    PIPE_COMPATIBLE,
  }

  private SnowflakeTelemetryPipeCreation preparePipeStartTelemetryEvent(
      String tableName, String stageName, String pipeName) {
    SnowflakeTelemetryPipeCreation result =
        new SnowflakeTelemetryPipeCreation(tableName, stageName, pipeName);
    boolean canListFiles = false;

    switch (checkPreRequisites()) {
      case PIPE_COMPATIBLE:
        result.setReusePipe(true);
      case STAGE_COMPATIBLE:
        result.setReuseStage(true);
        canListFiles = true;
      case TABLE_COMPATIBLE:
        result.setReuseTable(true);
      default:
        break;
    }

    if (canListFiles) {
      try {
        List<String> stageFiles = conn.listStage(stageName, prefix);
        result.setFileCountRestart(stageFiles.size());
      } catch (Exception err) {
        LOGGER.warn(
            "could not list remote stage {} - {}<{}>\n{}",
            stageName,
            err.getMessage(),
            err.getClass(),
            err.getStackTrace());
      }
      // at this moment, file processor does not know how many files should be reprocessed...
      result.setFileCountReprocessPurge(0);
    }

    return result;
  }

  private CleanerPrerequisites checkPreRequisites() {
    CleanerPrerequisites result = CleanerPrerequisites.NONE;
    Supplier<Boolean> tableCompatible =
        () -> conn.tableExist(tableName) && conn.isTableCompatible(tableName);
    Supplier<Boolean> stageCompatible =
        () -> conn.stageExist(stageName) && conn.isStageCompatible(stageName);
    Supplier<Boolean> pipeCompatible =
        () -> conn.pipeExist(pipeName) && conn.isPipeCompatible(tableName, stageName, pipeName);

    if (tableCompatible.get()) {
      result = CleanerPrerequisites.TABLE_COMPATIBLE;
      if (stageCompatible.get()) {
        result = CleanerPrerequisites.STAGE_COMPATIBLE;
        if (pipeCompatible.get()) {
          result = CleanerPrerequisites.PIPE_COMPATIBLE;
        }
      }
    }
    return result;
  }

  public static class FileCategorizer {
    private final Set<String> dirtyFiles = new HashSet<>();
    private final Map<String, IngestEntry> stageFiles = new HashMap<>();
    private final long currentOffset;
    private final FilteringPredicates predicates;

    static FileCategorizer build(
        Collection<String> files, long currentOffset, FilteringPredicates predicates) {
      FileCategorizer categorizer = new FileCategorizer(currentOffset, predicates);
      files.forEach(categorizer::categorizeFile);
      return categorizer;
    }

    private FileCategorizer(long startOffset, FilteringPredicates predicates) {
      this.currentOffset = startOffset;
      this.predicates = predicates;
    }

    private void categorizeFile(String file) {
      long fileOffset = FileNameUtils.fileNameToStartOffset(file);
      long timestamp = FileNameUtils.fileNameToTimeIngested(file);
      // if the file is stale (fileOffset > currentOffset) but file hasn't matured yet - give it a
      // chance to wait on stage. worst case - it will become stale and will be deleted slightly
      // later...
      if (fileOffset > currentOffset && predicates.matureTimestampPredicate.test(timestamp)) {
        dirtyFiles.add(file);
      } else {
        IngestEntry entry = new IngestEntry(InternalUtils.IngestedFileStatus.NOT_FOUND, timestamp);
        stageFiles.put(file, entry);
      }
    }

    void updateFileStatus(Map<String, IngestEntry> report) {
      report.forEach(
          (fileName, reportEntry) ->
              stageFiles.computeIfPresent(
                  fileName,
                  (key, entry) -> {
                    entry.status = reportEntry.status;
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

    // only for testing purposes!
    @VisibleForTesting ProcessorContext currentProcessorContext;
  }

  static class FilteringPredicates {
    @VisibleForTesting final Predicate<Long> matureTimestampPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> loadedFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> matureFilePredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> failedFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> staledFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> trackableFilesPredicate;
    @VisibleForTesting final Predicate<Map.Entry<String, IngestEntry>> oneHourOldEntryPredicate;
    @VisibleForTesting final Predicate<HistoryResponse.FileEntry> currentPartitionFilePredicate;

    public FilteringPredicates(TimeSupplier timeSupplier, String filePrefix) {
      // after each cycle we can end up with a set of files we should keep tracking - file,
      // which was either purged or moved to table storage will have this flag cleared - we don't
      // need to track it anymore
      trackableFilesPredicate = entry -> entry.getValue().keepTracking;

      // do not purge or delete files 'younger' than one minute - they may not have been processed
      // yet
      long oneMinute = Duration.ofSeconds(60).toMillis();
      matureTimestampPredicate = timestamp -> timestamp + oneMinute <= timeSupplier.currentTime();
      matureFilePredicate = entry -> matureTimestampPredicate.test(entry.getValue().timestamp);

      // loaded files - simple case - their ingest status is LOADED
      loadedFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  && matureFilePredicate.test(entry)
                  && entry.getValue().status == InternalUtils.IngestedFileStatus.LOADED;

      long oneHour = Duration.ofHours(1).toMillis();
      oneHourOldEntryPredicate =
          entry -> entry.getValue().timestamp <= timeSupplier.currentTime() - oneHour;

      // failed files, either:
      failedFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  && matureFilePredicate.test(entry)
                  // their status is partially loaded
                  && (entry.getValue().status == InternalUtils.IngestedFileStatus.PARTIALLY_LOADED
                      // or their status is failed
                      || entry.getValue().status == InternalUtils.IngestedFileStatus.FAILED
                      // or they are NOT loaded, but been sitting around for over one hour
                      || (oneHourOldEntryPredicate.test(entry)
                          && loadedFilesPredicate.negate().test(entry)));

      // stale files - this is potentially grey zone - the files have not yet been processed, they
      // are not marked as
      // failed yet, we don't know their status, but they are sitting in our register for over 10
      // minutes, but less than
      // one hour
      long tenMinutes = Duration.ofMinutes(10).toMillis();
      staledFilesPredicate =
          entry ->
              trackableFilesPredicate.test(entry)
                  && failedFilesPredicate.negate().test(entry)
                  && loadedFilesPredicate.negate().test(entry)
                  && entry.getValue().timestamp <= timeSupplier.currentTime() - tenMinutes;

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
  @VisibleForTesting
  static class ProcessorContext {
    final Set<String> files = new HashSet<>();
    final Map<String, IngestEntry> ingestHistory = new HashMap<>();
    final AtomicReference<String> historyMarker = new AtomicReference<>();
    final PipeProgressRegistryTelemetry progressTelemetry;

    final AtomicInteger cleanupCycle = new AtomicInteger(-1);
    long startTrackingHistoryTimestamp;

    private ProcessorContext(
        PipeProgressRegistryTelemetry progressTelemetry, long startTrackingHistoryTimestamp) {
      this.progressTelemetry = progressTelemetry;
      this.startTrackingHistoryTimestamp = startTrackingHistoryTimestamp;
    }
  }
}
