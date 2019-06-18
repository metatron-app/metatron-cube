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
import com.metamx.common.guava.Sequence;
import io.druid.common.guava.BytesRef;
import io.druid.common.utils.Sequences;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.python.google.common.primitives.Ints;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class BulkRow extends AbstractRow
{
  private static final LZ4FastDecompressor LZ4 = LZ4Factory.fastestInstance().fastDecompressor();

  private final int count;    // just for counting number of rows in local nodes
  private final Object[] values;

  @JsonCreator
  public BulkRow(@JsonProperty("count") int count, @JsonProperty("values") Object[] values)
  {
    this.count = count;
    this.values = values;
  }

  @JsonProperty
  public int count()
  {
    return count;
  }

  @JsonProperty
  public Object[] getValues()
  {
    return values;
  }

  public Sequence<Row> decompose()
  {
    final TimestampRLE timestamps = new TimestampRLE((byte[]) values[0]);
    for (int i = 1; i < values.length; i++) {
      if (values[i] instanceof byte[]) {
        final byte[] array = (byte[]) values[i];
        values[i] = new BytesInputStream(
            LZ4.decompress(array, Integer.BYTES, Ints.fromByteArray(array))
        );
      } else if (values[i] instanceof BytesRef) {
        final BytesRef array = (BytesRef) values[i];
        values[i] = new BytesInputStream(
            LZ4.decompress(array.bytes, Integer.BYTES, Ints.fromByteArray(array.bytes))
        );
      }
    }
    return Sequences.simple(
        new Iterable<Row>()
        {
          {
            for (int i = 1; i < values.length; i++) {
              if (values[i] instanceof BytesInputStream) {
                ((BytesInputStream) values[i]).reset();
              }
            }
          }

          @Override
          public Iterator<Row> iterator()
          {
            return new Iterator<Row>()
            {
              private final Iterator<Long> timestamp = timestamps.iterator();
              private int index;

              @Override
              public boolean hasNext()
              {
                return timestamp.hasNext();
              }

              @Override
              public Row next()
              {
                final int ix = index++;
                final Object[] row = new Object[values.length];
                row[0] = timestamp.next();
                for (int i = 1; i < row.length; i++) {
                  if (values[i] instanceof BytesInputStream) {
                    row[i] = ((BytesInputStream) values[i]).readVarSizeUTF();
                  } else {
                    row[i] = ((List) values[i]).get(ix);
                  }
                }
                return new CompactRow(row);
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
