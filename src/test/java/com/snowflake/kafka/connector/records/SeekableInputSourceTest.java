/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.records;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SeekableInputSourceTest
{
  @Test
  public void functionTest() throws IOException
  {
    String str = "test data";

    SeekableInputSource s = new SeekableInputSource(str.getBytes(StandardCharsets.UTF_8));

    assert s.length() == str.length();

    long p = str.length() - 4;

    s.seek(p);

    assert s.tell() == p;

    //read test
    String str2 = str.substring((int) p);

    byte[] str3 = new byte[4];
    int size = s.read(str3, 0, 4);

    assert size == 4;
    assert str2.equals(new String(str3, StandardCharsets.UTF_8));

    str3 = new byte[6];
    s.seek(p);
    size = s.read(str3, 1, 5);
    assert size == 4;
    assert str2.equals(new String(Arrays.copyOfRange(str3, 1, 5), StandardCharsets.UTF_8));
  }

  @Test(expected = IOException.class)
  public void closeMethodTest1() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes(StandardCharsets.UTF_8));
    s.close();
    s.length();
  }

  @Test(expected = IOException.class)
  public void closeMethodTest2() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes(StandardCharsets.UTF_8));
    s.close();
    s.tell();
  }

  @Test(expected = IOException.class)
  public void closeMethodTest3() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes(StandardCharsets.UTF_8));
    s.close();
    s.seek(12);
  }

  @Test(expected = IOException.class)
  public void closeMethodTest4() throws IOException
  {
    String str = "test";
    SeekableInputSource s = new SeekableInputSource(str.getBytes(StandardCharsets.UTF_8));
    s.close();
    s.read(new byte[10], 1,3);
  }


}
