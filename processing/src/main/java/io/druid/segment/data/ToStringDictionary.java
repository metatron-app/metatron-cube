/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.data;

import io.druid.common.guava.BufferRef;
import io.druid.common.utils.StringUtils;
import io.druid.segment.Tools;

import java.util.Iterator;

public class ToStringDictionary implements Dictionary<String>
{
  @Override
  public boolean isSorted()
  {
    return false;
  }

  @Override
  public Boolean containsNull()
  {
    return false;
  }

  @Override
  public String get(int index)
  {
    return String.valueOf(index);
  }

  @Override
  public byte[] getAsRaw(int index)
  {
    return StringUtils.toUtf8(String.valueOf(index));
  }

  @Override
  public BufferRef getAsRef(int index)
  {
    throw new UnsupportedOperationException("getAsRef");
  }

  @Override
  public int indexOf(String value, int start)
  {
    return StringUtils.isNullOrEmpty(value) ? -1 : Integer.valueOf(value);
  }

  @Override
  public int size()
  {
    return -1;
  }

  @Override
  public long getSerializedSize()
  {
    return 0;
  }

  @Override
  public Iterator<String> iterator()
  {
    throw new UnsupportedOperationException("iterator");
  }

  @Override
  public <R> R apply(int index, Tools.Function<R> function)
  {
    throw new UnsupportedOperationException("apply");
  }

  @Override
  public void scan(int index, Tools.Scanner scanner)
  {
    throw new UnsupportedOperationException("scan");
  }

  @Override
  public void scan(Tools.Scanner scanner)
  {
    throw new UnsupportedOperationException("scan");
  }

  @Override
  public void close()
  {
  }
}
