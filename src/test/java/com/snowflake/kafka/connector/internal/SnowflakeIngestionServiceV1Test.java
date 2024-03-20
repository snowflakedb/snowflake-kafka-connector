package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import net.snowflake.ingest.SimpleIngestManager;
import net.snowflake.ingest.connection.HistoryResponse;
import net.snowflake.ingest.connection.IngestStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class SnowflakeIngestionServiceV1Test {

  private SimpleIngestManager manager;
  private SnowflakeIngestionService svc;

  @BeforeEach
  void setup() {
    manager = Mockito.mock(SimpleIngestManager.class);

    svc = new SnowflakeIngestionServiceV1("stageName", "pipeName", manager, null);
  }

  @Test
  void readIngestHistoryForward_willFetchDataPassingProvidedParameters() throws Exception {

    Map<String, InternalUtils.IngestedFileStatus> history = new HashMap<>();
    AtomicReference<String> historyMarker = new AtomicReference<>();

    when(manager.getHistory(null, 123, null))
        .thenReturn(file1Response("first", IngestStatus.LOAD_IN_PROGRESS));
    when(manager.getHistory(null, 123, "first"))
        .thenReturn(file2Response("final", IngestStatus.LOAD_IN_PROGRESS));

    int read1 = svc.readIngestHistoryForward(history, f -> true, historyMarker, 123);
    int read2 = svc.readIngestHistoryForward(history, f -> true, historyMarker, 123);

    assertThat(read1).isEqualTo(1);
    assertThat(read2).isEqualTo(1);

    assertThat(historyMarker.get()).isEqualTo("final");
    assertThat(history)
        .hasSize(2)
        .containsKey("test1.txt")
        .containsKey("test2.txt")
        .containsValues(InternalUtils.IngestedFileStatus.LOAD_IN_PROGRESS);
  }

  @Test
  void readIngestHistoryForward_willUpdateHistoryEntryIfUpdated() throws Exception {

    Map<String, InternalUtils.IngestedFileStatus> history = new HashMap<>();
    AtomicReference<String> historyMarker = new AtomicReference<>();

    when(manager.getHistory(null, 123, null))
        .thenReturn(file1Response("first", IngestStatus.LOAD_IN_PROGRESS));
    when(manager.getHistory(null, 123, "first"))
        .thenReturn(file1Response("final", IngestStatus.LOADED));

    int read1 = svc.readIngestHistoryForward(history, f -> true, historyMarker, 123);
    int read2 = svc.readIngestHistoryForward(history, f -> true, historyMarker, 123);

    assertThat(read1).isEqualTo(1);
    assertThat(read2).isEqualTo(1);

    assertThat(historyMarker.get()).isEqualTo("final");
    assertThat(history)
        .hasSize(1)
        .containsKey("test1.txt")
        .containsValues(InternalUtils.IngestedFileStatus.LOADED);
  }

  @Test
  void readIngestHistoryForward_loadOnlyFilesMatchingPredicate() throws Exception {

    Map<String, InternalUtils.IngestedFileStatus> history = new HashMap<>();
    AtomicReference<String> historyMarker = new AtomicReference<>();

    when(manager.getHistory(null, 123, null))
        .thenReturn(file1Response("first", IngestStatus.LOADED));
    when(manager.getHistory(null, 123, "first"))
        .thenReturn(file2Response("final", IngestStatus.LOADED));

    int read1 =
        svc.readIngestHistoryForward(history, f -> f.getPath().contains("1"), historyMarker, 123);
    int read2 =
        svc.readIngestHistoryForward(history, f -> f.getPath().contains("1"), historyMarker, 123);

    assertThat(read1).isEqualTo(1);
    assertThat(read2).isEqualTo(0);

    assertThat(historyMarker.get()).isEqualTo("final");
    assertThat(history)
        .hasSize(1)
        .containsKey("test1.txt")
        .containsValues(InternalUtils.IngestedFileStatus.LOADED);
  }

  private HistoryResponse file1Response(String marker, IngestStatus fileStatus) {
    HistoryResponse response = new HistoryResponse();
    response.setNextBeginMark(marker);
    HistoryResponse.FileEntry file1 = new HistoryResponse.FileEntry();
    file1.setPath("test1.txt");
    file1.setStatus(fileStatus);
    response.files.add(file1);
    return response;
  }

  private HistoryResponse file2Response(String marker, IngestStatus fileStatus) {
    HistoryResponse response = new HistoryResponse();
    response.setNextBeginMark(marker);
    HistoryResponse.FileEntry file1 = new HistoryResponse.FileEntry();
    file1.setPath("test2.txt");
    file1.setStatus(fileStatus);
    response.files.add(file1);
    return response;
  }
}
