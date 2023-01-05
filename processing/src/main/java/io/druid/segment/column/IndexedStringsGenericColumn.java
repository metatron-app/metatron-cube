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

package io.druid.segment.column;

import io.druid.common.guava.BufferRef;
import io.druid.data.ValueDesc;
import io.druid.segment.Tools;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import org.roaringbitmap.IntIterator;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 */
public class IndexedStringsGenericColumn implements GenericColumn, Indexed.Scannable<String>
{
  private final GenericIndexed<String> indexed;
  private final CompressionStrategy compressionType;

  public IndexedStringsGenericColumn(GenericIndexed<String> indexed, CompressionStrategy compressionType)
  {
    this.indexed = indexed;
    this.compressionType = compressionType;
  }

  @Override
  public ValueDesc getType()
  {
    return ValueDesc.STRING;
  }

  @Override
  public CompressionStrategy compressionType()
  {
    return compressionType;
  }

  @Override
  public int size()
  {
    return indexed.size();
  }

  @Override
  public String getString(int rowNum)
  {
    return indexed.get(rowNum);
  }

  @Override
  public Object getValue(int rowNum)
  {
    return getString(rowNum);
  }

  @Override
  public String get(int index)
  {
    return getString(index);
  }

  @Override
  public BufferRef getAsRef(int index)
  {
    return indexed.getAsRef(index);
  }

  @Override
  public void scan(IntIterator iterator, Tools.Scanner scanner)
  {
    indexed.scan(iterator, scanner);
  }

  @Override
  public void scan(int index, Tools.Scanner scanner)
  {
    indexed.scan(index, scanner);
  }

  @Override
  public void scan(IntIterator iterator, Tools.ObjectScanner<String> scanner)
  {
    indexed.scan(iterator, scanner);
  }

  @Override
  public <R> Stream<R> apply(IntIterator iterator, Tools.Function<R> function)
  {
    return indexed.apply(iterator, function);
  }

  @Override
  public <R> R apply(int index, Tools.Function<R> function)
  {
    return indexed.apply(index, function);
  }

  @Override
  public Iterator<String> iterator()
  {
    return indexed.iterator();
  }
}
