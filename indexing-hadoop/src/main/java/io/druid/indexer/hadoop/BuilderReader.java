/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer.hadoop;

import java.io.Reader;

/**
 */
public class BuilderReader extends Reader
{
  private StringBuilder builder;
  private int pos;
  private int mark;

  public BuilderReader(StringBuilder builder)
  {
    this.builder = builder;
  }

  public void close()
  {
  }

  @Override
  public int read()
  {
    return !ready() ? -1 : builder.charAt(pos++);
  }

  @Override
  public int read(char[] b, int off, int len)
  {
    if (pos >= builder.length()) {
      return -1;
    }
    len = Math.min(len, builder.length() - pos);
    builder.getChars(pos, pos + len, b, off);
    pos += len;
    return len;
  }

  @Override
  public long skip(long n)
  {
    if ((long) pos + n > (long) builder.length()) {
      n = (long) (builder.length() - pos);
    }
    pos = (int) ((long) pos + n);
    return n;
  }

  @Override
  public boolean ready()
  {
    return pos < builder.length();
  }

  @Override
  public boolean markSupported()
  {
    return true;
  }

  @Override
  public void mark(int readAheadLimit)
  {
    mark = pos;
  }

  @Override
  public void reset()
  {
    pos = mark;
  }
}
