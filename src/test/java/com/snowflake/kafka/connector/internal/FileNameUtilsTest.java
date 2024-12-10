package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.FileNameUtils.*;
import static com.snowflake.kafka.connector.internal.FileNameUtils.searchForMissingOffsets;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class FileNameUtilsTest {

  @Test
  public void testFileNameFunctions() throws InterruptedException {
    int partition = 123;
    long startOffset = 456L;
    long endOffset = 789L;
    String tableName = "test_table";
    long time1 = System.currentTimeMillis();
    Thread.sleep(5); // error in maven without sleep
    String fileName =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, "", partition, startOffset, endOffset);
    Thread.sleep(5);
    long time2 = System.currentTimeMillis();

    assertThat(FileNameUtils.verifyFileName("asdasdasdasdsa.json.gz")).isFalse();
    assertThat(FileNameUtils.verifyFileName(fileName)).isTrue();
    assertThat(FileNameUtils.fileNameToStartOffset(fileName)).isEqualTo(startOffset);
    assertThat(FileNameUtils.fileNameToEndOffset(fileName)).isEqualTo(endOffset);
    assertThat(FileNameUtils.fileNameToPartition(fileName)).isEqualTo(partition);

    long createTime = FileNameUtils.fileNameToTimeIngested(fileName);
    assertThat((createTime > time1) && (createTime < time2)).isTrue();

    assertThat(FileNameUtils.removePrefixAndGZFromFileName("A/B/C/abc.tar.gz"))
        .isEqualTo("abc.tar");
    assertThat(FileNameUtils.removePrefixAndGZFromFileName("A/B/C/abc.json")).isEqualTo("abc.json");
    assertThat(FileNameUtils.getPrefixFromFileName("A/B/C/abc.tar.gz")).isEqualTo("A/B/C");
    assertThat(FileNameUtils.getPrefixFromFileName("A/B/C/abc.json")).isEqualTo("A/B/C");
    assertThat(FileNameUtils.getPrefixFromFileName("abc.json")).isNull();
    assertThat(FileNameUtils.getPrefixFromFileName("abc.json.gz")).isNull();
    assertThatThrownBy(() -> FileNameUtils.getPrefixFromFileName("A/B/C/"));
    assertThatThrownBy(() -> FileNameUtils.getPrefixFromFileName(""));

    String brokenFileName =
        FileNameTestUtils.brokenRecordFileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, "", partition, startOffset, true);
    assertThat(TestUtils.verifyBrokenRecordName(brokenFileName)).isTrue();

    brokenFileName =
        FileNameTestUtils.brokenRecordFileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, "", partition, startOffset, false);
    assertThat(TestUtils.verifyBrokenRecordName(brokenFileName)).isTrue();
  }

  @Test
  public void testFileExpiration() {
    long time = System.currentTimeMillis();
    String expiredFile =
        "test_app/test_topic/1/123_456_"
            + (time - InternalUtils.MAX_RECOVERY_TIME - 3600 * 1000)
            + ".json.gz";

    String unexpiredFile =
        "test_app/test_topic/1/123_456_"
            + (time - InternalUtils.MAX_RECOVERY_TIME + 3600 * 1000)
            + ".json.gz";

    assertThat(FileNameTestUtils.isFileExpired(expiredFile)).isTrue();
    assertThat(FileNameTestUtils.isFileExpired(unexpiredFile)).isFalse();
  }

  @Test
  public void testFileNameWillEncodeTopicNameToCreateUniquePrefix() {
    int partition = 123;
    long startOffset = 456L;
    long endOffset = 789L;
    String tableName = "test_table";
    String topicName = "test_topic";
    long now = System.currentTimeMillis();
    String fileName =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, topicName, partition, startOffset, endOffset);

    assertThat(fileName).isNotNull();
    assertThat(FileNameUtils.verifyFileName(fileName)).isTrue();
    assertThat(FileNameUtils.fileNameToPartition(fileName)).isEqualTo(partition);
    assertThat(FileNameUtils.fileNameToStartOffset(fileName)).isEqualTo(startOffset);
    assertThat(FileNameUtils.fileNameToEndOffset(fileName)).isEqualTo(endOffset);
    assertThat(FileNameUtils.fileNameToTimeIngested(fileName))
        .isGreaterThanOrEqualTo(now - 5000L)
        .isLessThanOrEqualTo(now + 5000L);

    String prefix = FileNameUtils.getPrefixFromFileName(fileName);
    // without bit shifting - the expression would match \d\d\d expression, but since we are left
    // shifting by 16 bits left, it is going to be a larger number
    assertThat(prefix).matches("^TEST_CONNECTOR/test_table/\\d\\d\\d\\d\\d\\d\\d\\d\\d.*");
  }

  @Test
  public void testFileNameWillEncodeTopicNameToCreateUniquePrefixesAndNamesWontCollide() {
    int partition = 123;
    long startOffset = 456L;
    long endOffset = 789L;
    String tableName = "test_table";
    String topicName1 = "test_topic1";
    String topicName2 = "test_topic2";
    String fileName1 =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME,
            tableName,
            topicName1,
            partition,
            startOffset,
            endOffset);
    String fileName2 =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME,
            tableName,
            topicName2,
            partition,
            startOffset,
            endOffset);
    String fileName3 =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, "", partition, startOffset, endOffset);
    String fileName4 =
        FileNameTestUtils.fileName(
            TestUtils.TEST_CONNECTOR_NAME, tableName, "", partition, startOffset, endOffset);

    String prefix1 = FileNameUtils.getPrefixFromFileName(fileName1);
    String prefix2 = FileNameUtils.getPrefixFromFileName(fileName2);
    String prefix3 = FileNameUtils.getPrefixFromFileName(fileName3);
    String prefix4 = FileNameUtils.getPrefixFromFileName(fileName4);

    // prefixes with topic name included won't match
    assertThat(prefix1).isNotEqualTo(prefix2);
    // but prefixes without topic name would match
    assertThat(prefix3).isEqualTo(prefix4);
  }

  @Test
  public void testFileNameWontSupportMoreThan32767Partitions() {
    int partition = 0x8000;
    long startOffset = 456L;
    long endOffset = 789L;
    String tableName = "test_table";
    String topicName = "test_topic";
    assertThatThrownBy(
            () ->
                FileNameTestUtils.fileName(
                    TestUtils.TEST_CONNECTOR_NAME,
                    tableName,
                    topicName,
                    partition,
                    startOffset,
                    endOffset))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @MethodSource("testData")
  public void testSearchForMissingOffsets(
      List<String> fileNames, String expectedContinuousOffsets, String expectedMissingOffsets) {
    // when
    OffsetContinuityRanges result = searchForMissingOffsets(fileNames);

    // then
    assertThat(result.getContinuousOffsets()).isEqualTo(expectedContinuousOffsets);
    assertThat(result.getMissingOffsets()).isEqualTo(expectedMissingOffsets);
  }

  public static Stream<Arguments> testData() {
    int partition = 123;
    String tableName = "test_table";
    String filePrefix = filePrefix(TestUtils.TEST_CONNECTOR_NAME, tableName, "topic", partition);

    return Stream.of(
        Arguments.of(Collections.emptyList(), "[]", "[]"),
        Arguments.of(Collections.singletonList(fileName(filePrefix, 0, 10)), "[[0,10]]", "[]"),
        Arguments.of(
            Arrays.asList(fileName(filePrefix, 0, 10), fileName(filePrefix, 100, 2137)),
            "[[0,10][100,2137]]",
            "[[11,99]]"),
        Arguments.of(
            Arrays.asList(
                fileName(filePrefix, 0, 10),
                fileName(filePrefix, 11, 20),
                fileName(filePrefix, 21, 100),
                fileName(filePrefix, 101, 1991)),
            "[[0,10][11,20][21,100][101,1991]]",
            "[]"),
        Arguments.of(
            Arrays.asList(
                fileName(filePrefix, 0, 10),
                fileName(filePrefix, 11, 20),
                fileName(filePrefix, 21, 100),
                fileName(filePrefix, 101, 1991),
                fileName(filePrefix, 1996, 2000),
                fileName(filePrefix, 2001, 2024)),
            "[[0,10][11,20][21,100][101,1991][1996,2000][2001,2024]]",
            "[[1992,1995]]"),
        Arguments.of(
            Arrays.asList(
                fileName(filePrefix, 1996, 2000),
                fileName(filePrefix, 11, 20),
                fileName(filePrefix, 21, 100),
                fileName(filePrefix, 2001, 2024),
                fileName(filePrefix, 101, 1991),
                fileName(filePrefix, 0, 10)),
            "[[0,10][11,20][21,100][101,1991][1996,2000][2001,2024]]",
            "[[1992,1995]]"));
  }
}
