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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.Sequences;
import io.druid.java.util.common.guava.Sequence;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BulkRow extends AbstractRow
{
  private static final LZ4FastDecompressor LZ4 = LZ4Factory.fastestInstance().fastDecompressor();

  private final int timeIndex;
  private final int count;    // just for counting number of rows in local nodes
  private final Object[] values;

  @JsonCreator
  public BulkRow(
      @JsonProperty("count") int count,
      @JsonProperty("values") Object[] values,
      @JsonProperty("timeIndex") int timeIndex
  )
  {
    this.count = count;
    this.timeIndex = timeIndex;
    this.values = values;
  }

  @JsonProperty
  public int count()
  {
    return count;
  }

  @JsonProperty
  public int timeIndex()
  {
    return timeIndex;
  }

  @JsonProperty
  public Object[] values()
  {
    return values;
  }

  public Sequence<Object[]> decompose()
  {
    final TimestampRLE timestamps;
    if (timeIndex >= 0) {
      timestamps = new TimestampRLE((byte[]) values[timeIndex]);
      values[timeIndex] = null;
    } else {
      timestamps = null;
    }
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof byte[]) {
        final byte[] array = (byte[]) values[i];
        values[i] = new BytesInputStream(LZ4.decompress(array, Ints.BYTES, Ints.fromByteArray(array)));
      } else if (values[i] instanceof BytesRef) {
        final BytesRef array = (BytesRef) values[i];
        values[i] = new BytesInputStream(LZ4.decompress(array.bytes, Ints.BYTES, Ints.fromByteArray(array.bytes)));
      }
    }
    return Sequences.simple(
        new Iterable<Object[]>()
        {
          {
            for (int i = 0; i < values.length; i++) {
              if (i == timeIndex) {
                values[i] = timestamps.iterator();
              } else if (values[i] instanceof BytesInputStream) {
                ((BytesInputStream) values[i]).reset();
              }
            }
          }

          @Override
          public Iterator<Object[]> iterator()
          {
            return new Iterator<Object[]>()
            {
              private int index;

              @Override
              public boolean hasNext()
              {
                return index < count;
              }

              @Override
              public Object[] next()
              {
                final int ix = index++;
                final Object[] row = new Object[values.length];
                for (int i = 0; i < row.length; i++) {
                  if (values[i] instanceof Iterator) {
                    Iterator iterator = (Iterator) values[i];
                    row[i] = iterator.hasNext() ? iterator.next() : -1;
                  } else if (values[i] instanceof BytesInputStream) {
                    row[i] = ((BytesInputStream) values[i]).readVarSizeUTF();
                  } else {
                    row[i] = ((List) values[i]).get(ix);
                  }
                }
                return row;
              }
            };
          }
        }
    );
  }

  @Override
  public Object getRaw(String dimension)
  {
    throw new UnsupportedOperationException("getRaw");
  }

  @Override
  public Collection<String> getColumns()
  {
    throw new UnsupportedOperationException("getColumns");
  }
}
