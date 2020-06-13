package com.snowflake.kafka.connector.internal;

import org.junit.Test;

public class FileNameUtilsTest
{
  @Test
  public void testFileNameFunctions() throws InterruptedException
  {
    int partition = 123;
    long startOffset = 456L;
    long endOffset = 789L;
    String topic = "test_topic";
    long time1 = System.currentTimeMillis();
    Thread.sleep(5);//error in maven without sleep
    String fileName = FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, topic, partition, startOffset, endOffset);
    Thread.sleep(5);
    long time2 = System.currentTimeMillis();

    assert !FileNameUtils.verifyFileName("asdasdasdasdsa.json.gz");
    assert FileNameUtils.verifyFileName(fileName);
    assert FileNameUtils.fileNameToStartOffset(fileName) == startOffset;
    assert FileNameUtils.fileNameToEndOffset(fileName) == endOffset;
    assert FileNameUtils.fileNameToPartition(fileName) == partition;

    long createTime = FileNameUtils.fileNameToTimeIngested(fileName);
    assert (createTime > time1) && (createTime < time2);

    assert FileNameUtils.removePrefixAndGZFromFileName("A/B/C/abc.tar.gz").equals("abc.tar");
    assert FileNameUtils.removePrefixAndGZFromFileName("A/B/C/abc.json").equals("abc.json");
    assert FileNameUtils.getPrefixFromFileName("A/B/C/abc.tar.gz").equals("A/B/C");
    assert FileNameUtils.getPrefixFromFileName("A/B/C/abc.json").equals("A/B/C");
    assert FileNameUtils.getPrefixFromFileName("abc.json") == null;
    assert FileNameUtils.getPrefixFromFileName("abc.json.gz") == null;
    try {
      FileNameUtils.getPrefixFromFileName("A/B/C/");
      assert false;
    } catch (Exception e) {
    }
    try {
      FileNameUtils.getPrefixFromFileName("");
      assert false;
    } catch (Exception e) {
    }

    String brokenFileName = FileNameUtils.brokenRecordFileName(TestUtils.TEST_CONNECTOR_NAME, topic, partition, startOffset, true);
    assert TestUtils.verifyBrokenRecordName(brokenFileName);

    brokenFileName = FileNameUtils.brokenRecordFileName(TestUtils.TEST_CONNECTOR_NAME, topic, partition, startOffset, false);
    assert TestUtils.verifyBrokenRecordName(brokenFileName);
  }

  @Test
  public void testFileExpiration()
  {
    long time = System.currentTimeMillis();
    String expiredFile = "test_app/test_topic/1/123_456_" +
      (time - InternalUtils.MAX_RECOVERY_TIME - 3600 * 1000) + ".json.gz";

    String unexpiredFile = "test_app/test_topic/1/123_456_" +
      (time - InternalUtils.MAX_RECOVERY_TIME + 3600 * 1000) + ".json.gz";

    assert FileNameUtils.isFileExpired(expiredFile);

    assert !FileNameUtils.isFileExpired(unexpiredFile);
  }

}
