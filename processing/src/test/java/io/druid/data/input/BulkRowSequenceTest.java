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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import io.druid.common.Yielders;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class BulkRowSequenceTest
{
  @Test
  public void test() throws IOException
  {
    Sequence<Object[]> rows = Sequences.simple(Arrays.<Object[]>asList(
        cr(0), cr(1), cr(2), cr(3), cr(4)
    ));
    Sequence<BulkRow> bulk = Sequences.map(
        new BulkRowSequence(rows, Arrays.asList(ValueDesc.LONG, ValueDesc.STRING), 0, 2),
        tagged -> new BulkRow(tagged.tag(), tagged.value(), 0)
    );

    final List<long[]> longs = Sequences.toList(Sequences.map(
        bulk, new Function<BulkRow, long[]>()
        {
          @Override
          public long[] apply(BulkRow input)
          {
            List<Long> timestamps = Lists.newArrayList();
            for (Object[] row : Sequences.toList(input.decompose())) {
              timestamps.add((Long) row[0]);
            }
            return Longs.toArray(timestamps);
          }
        }
    ));
    Assert.assertArrayEquals(new long[]{0, 1}, longs.get(0));
    Assert.assertArrayEquals(new long[]{2, 3}, longs.get(1));
    Assert.assertArrayEquals(new long[]{4}, longs.get(2));

    final List<String[]> strings = Sequences.toList(Sequences.map(
        bulk, new Function<BulkRow, String[]>()
        {
          @Override
          public String[] apply(BulkRow input)
          {
            List<String> strings = Lists.newArrayList();
            for (Object[] row : Sequences.toList(input.decompose())) {
              strings.add((String)row[1]);
            }
            return strings.toArray(new String[0]);
          }
        }
    ));
    Assert.assertArrayEquals(new String[]{"0", "1"}, strings.get(0));
    Assert.assertArrayEquals(new String[]{"2", "3"}, strings.get(1));
    Assert.assertArrayEquals(new String[]{"4"}, strings.get(2));

    Yielder<BulkRow> yielder = bulk.toYielder(null, new Yielders.Yielding<BulkRow>());
    List<Long> timestamps = Lists.newArrayList();
    for (Object[] row : Sequences.toList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertArrayEquals(new long[]{0, 1}, Longs.toArray(timestamps));
    yielder = yielder.next(null);
    timestamps.clear();
    for (Object[] row : Sequences.toList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertArrayEquals(new long[]{2, 3}, Longs.toArray(timestamps));
    yielder = yielder.next(null);
    timestamps.clear();
    for (Object[] row : Sequences.toList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertTrue(yielder.isDone());

    DefaultObjectMapper mapper = new DefaultObjectMapper(new SmileFactory());
    byte[] s = mapper.writeValueAsBytes(bulk);
    List<Row> deserialized = mapper.readValue(s, new TypeReference<List<Row>>() {});
    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(0), cr(1)}),
        GuavaUtils.arrayOfArrayToString(Sequences.toList(((BulkRow) deserialized.get(0)).decompose()).toArray(new Object[0][]))
    );
    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(2), cr(3)}),
        GuavaUtils.arrayOfArrayToString(Sequences.toList(((BulkRow) deserialized.get(1)).decompose()).toArray(new Object[0][]))
    );
    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(4)}),
        GuavaUtils.arrayOfArrayToString(Sequences.toList(((BulkRow) deserialized.get(2)).decompose()).toArray(new Object[0][]))
    );
  }

  private Object[] cr(long x)
  {
    return new Object[]{x, String.valueOf(x)};
  }
}