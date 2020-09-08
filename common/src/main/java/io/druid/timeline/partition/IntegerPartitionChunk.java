/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.timeline.partition;

/**
 */
public class IntegerPartitionChunk<T> implements PartitionChunk<T>
{
  private final Integer start;
  private final Integer end;
  private final int chunkNumber;
  private final T object;

  public static <T> IntegerPartitionChunk<T> make(Integer start, Integer end, int chunkNumber, T obj)
  {
    return new IntegerPartitionChunk<T>(start, end, chunkNumber, obj);
  }

  public IntegerPartitionChunk(
      Integer start,
      Integer end,
      int chunkNumber,
      T object
  )
  {
    this.start = start;
    this.end = end;
    this.chunkNumber = chunkNumber;
    this.object = object;
  }

  @Override
  public T getObject()
  {
    return object;
  }

  @Override
  public boolean abuts(PartitionChunk<T> chunk)
  {
    if (chunk instanceof IntegerPartitionChunk) {
      IntegerPartitionChunk<T> intChunk = (IntegerPartitionChunk<T>) chunk;

      if (intChunk.isStart()) {
        return false;
      }
      return intChunk.start.equals(end);
    }

    return false;
  }

  @Override
  public boolean isStart()
  {
    return start == null;
  }

  @Override
  public boolean isEnd()
  {
    return end == null;
  }

  @Override
  public int getChunkNumber()
  {
    return chunkNumber;
  }

  @Override
  public int compareTo(PartitionChunk<T> chunk)
  {
    if (chunk instanceof IntegerPartitionChunk) {
      IntegerPartitionChunk<T> intChunk = (IntegerPartitionChunk<T>) chunk;
      return Integer.compare(chunkNumber, intChunk.chunkNumber);
    } else {
      throw new IllegalArgumentException("Cannot compare against something that is not an IntegerPartitionChunk.");
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return chunkNumber == ((IntegerPartitionChunk) o).chunkNumber;
  }

  @Override
  public int hashCode()
  {
    return chunkNumber;
  }

  @Override
  public String toString()
  {
    return "IntegerPartitionChunk{" +
           "start=" + start +
           ", end=" + end +
           ", object=" + object +
           '}';
  }
}

