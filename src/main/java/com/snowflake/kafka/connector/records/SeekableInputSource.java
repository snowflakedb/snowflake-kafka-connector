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

import com.snowflake.kafka.connector.internal.Logging;
import org.apache.avro.file.SeekableInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Seekable input source for parsing AVRO data from memory.
 * Max data size is 2GB.
 */
public class SeekableInputSource implements SeekableInput
{
  private static Logger LOGGER =
      LoggerFactory.getLogger(SeekableInputSource.class.getName());

  private byte[] source;
  private int length;
  private int pointer;

  /**
   * constructor, create a seekable input source from byte array
   * @param bytes byte array
   */
  public SeekableInputSource(byte[] bytes)
  {
    this.source = bytes;
    this.length = bytes.length;
    this.pointer = 0;
  }

  /**
   * move pointer
   * @param p new offset, positive, integer number
   * @throws IOException if source is closed or input offset is invalid
   */
  @Override
  public void seek(final long p) throws IOException
  {
    if(source == null)
    {
      throw new IOException("Source closed");
    }
    if(p < 0 || p > Integer.MAX_VALUE)
    {
      throw new IOException("Invalid offset " + p);
    }
    this.pointer = (int) p;

    LOGGER.debug(Logging.logMessage("pointer moved to {}", p));
  }

  /**
   * retrieve current offset
   * @return a non negative number
   * @throws IOException if source is closed
   */
  @Override
  public long tell() throws IOException
  {
    if(source == null)
    {
      throw new IOException("Source closed");
    }
    return pointer;
  }

  /**
   * retrieve file length
   * @return a non negative number
   * @throws IOException if source is closed
   */
  @Override
  public long length() throws IOException
  {
    if(source == null)
    {
      throw new IOException("Source closed");
    }
    return length;
  }

  /**
   * read bytes into given bytes array
   * @param b output array
   * @param off start offset
   * @param len max length of reading source
   * @return length of bytes have been read, -1 if EOF
   * @throws IOException if source is closed
   */
  @Override
  public int read(final byte[] b, final int off, final int len) throws
      IOException
  {
    if(source == null)
    {
      throw new IOException("Source closed");
    }
    if(pointer == length)
    {
      return -1;
    }

    int size = 0;
    while(size + pointer < length && size < len)
    {

      b[off + size] = source[pointer + size];
      size ++;
    }
    pointer += size;
    return size;
  }

  /**
   * close source object
   */
  @Override
  public void close()
  {
    this.source = null;
  }
}
