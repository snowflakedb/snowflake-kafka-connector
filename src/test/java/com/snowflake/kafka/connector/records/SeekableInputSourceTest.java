package com.snowflake.kafka.connector.records;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class SeekableInputSourceTest
{
  @Test
  public void functionTest() throws IOException
  {
    String str = "test data";

    SeekableInputSource s = new SeekableInputSource(str.getBytes());

    assert s.length() == str.length();

    long p = str.length() - 4;

    s.seek(p);

    assert s.tell() == p;

    //read test
    String str2 = str.substring((int) p);

    byte[] str3 = new byte[4];
    int size = s.read(str3, 0, 4);

    assert size == 4;
    assert str2.equals(new String(str3));

    str3 = new byte[6];
    s.seek(p);
    size = s.read(str3, 1, 5);
    assert size == 4;
    assert str2.equals(new String(Arrays.copyOfRange(str3, 1, 5)));
  }

  @Test(expected = IOException.class)
  public void closeMethodTest1() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes());
    s.close();
    s.length();
  }

  @Test(expected = IOException.class)
  public void closeMethodTest2() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes());
    s.close();
    s.tell();
  }

  @Test(expected = IOException.class)
  public void closeMethodTest3() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes());
    s.close();
    s.seek(12);
  }

  @Test(expected = IOException.class)
  public void closeMethodTest4() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes());
    s.close();
    s.read(new byte[10], 1,3);
  }


}
