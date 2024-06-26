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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import io.druid.common.Yielders;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.RowSignature;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class BulkSequenceTest
{
  private static RowSignature schema(ValueDesc... descs)
  {
    List<ValueDesc> columnTypes = Arrays.asList(descs);
    List<String> columnNames = Lists.newArrayList();
    for (int i = 0; i < columnTypes.size(); i++) {
      columnNames.add("col" + i);
    }
    return RowSignature.of(columnNames, columnTypes);
  }

  @Test
  public void test() throws IOException
  {
    Sequence<Object[]> rows = Sequences.simple(cr(0), cr(1), cr(2), cr(3), cr(4));

    Sequence<BulkRow> sequence = seq(rows, 2, ValueDesc.LONG, ValueDesc.FLOAT, ValueDesc.DOUBLE, ValueDesc.STRING);

    final List<long[]> longs = GuavaUtils.transform(
        serde(sequence), new Function<BulkRow, long[]>()
        {
          @Override
          public long[] apply(BulkRow input)
          {
            List<Long> timestamps = Lists.newArrayList();
            for (Object[] row : Lists.newArrayList(input.decompose())) {
              timestamps.add((Long) row[0]);
            }
            return Longs.toArray(timestamps);
          }
        }
    );
    Assert.assertArrayEquals(new long[]{0, 1}, longs.get(0));
    Assert.assertArrayEquals(new long[]{2, 3}, longs.get(1));
    Assert.assertArrayEquals(new long[]{4}, longs.get(2));

    final List<float[]> floats = GuavaUtils.transform(
        serde(sequence), new Function<BulkRow, float[]>()
        {
          @Override
          public float[] apply(BulkRow input)
          {
            List<Float> floatList = Lists.newArrayList();
            for (Object[] row : Lists.newArrayList(input.decompose())) {
              floatList.add((Float) row[1]);
            }
            return Floats.toArray(floatList);
          }
        }
    );
    Assert.assertArrayEquals(new float[]{0f, 1f}, floats.get(0), 0.0000000000001f);
    Assert.assertArrayEquals(new float[]{2f, 3f}, floats.get(1), 0.0000000000001f);
    Assert.assertArrayEquals(new float[]{4f}, floats.get(2), 0.0000000000001f);

    final List<double[]> doubles = GuavaUtils.transform(
        serde(sequence), new Function<BulkRow, double[]>()
        {
          @Override
          public double[] apply(BulkRow input)
          {
            List<Double> doubleList = Lists.newArrayList();
            for (Object[] row : Lists.newArrayList(input.decompose())) {
              doubleList.add((Double) row[2]);
            }
            return Doubles.toArray(doubleList);
          }
        }
    );
    Assert.assertArrayEquals(new double[]{0d, 1d}, doubles.get(0), 0.0000000000001d);
    Assert.assertArrayEquals(new double[]{2d, 3d}, doubles.get(1), 0.0000000000001d);
    Assert.assertArrayEquals(new double[]{4d}, doubles.get(2), 0.0000000000001d);

    final List<String[]> strings = GuavaUtils.transform(
        serde(sequence), new Function<BulkRow, String[]>()
        {
          @Override
          public String[] apply(BulkRow input)
          {
            List<String> strings = Lists.newArrayList();
            for (Object[] row : Lists.newArrayList(input.decompose())) {
              strings.add((String) row[3]);
            }
            return strings.toArray(new String[0]);
          }
        }
    );
    Assert.assertArrayEquals(new String[]{"0", "1"}, strings.get(0));
    Assert.assertArrayEquals(new String[]{"2", "3"}, strings.get(1));
    Assert.assertArrayEquals(new String[]{"4"}, strings.get(2));

    List<Long> timestamps = Lists.newArrayList();
    Yielder<BulkRow> yielder = Yielders.each(seqSerde(sequence));
    Assert.assertFalse(yielder.isDone());
    for (Object[] row : Lists.newArrayList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertArrayEquals(new long[]{0, 1}, Longs.toArray(timestamps));

    timestamps.clear();
    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    for (Object[] row : Lists.newArrayList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertArrayEquals(new long[]{2, 3}, Longs.toArray(timestamps));

    timestamps.clear();
    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    for (Object[] row : Lists.newArrayList(yielder.get().decompose())) {
      timestamps.add((Long) row[0]);
    }
    Assert.assertArrayEquals(new long[]{4}, Longs.toArray(timestamps));

    timestamps.clear();
    Assert.assertTrue(yielder.next(null).isDone());
  }

  @Test
  public void testSerde() throws IOException
  {
    Sequence<Object[]> rows = Sequences.simple(cr(0), cr(1), cr(2), cr(3), cr(4));
    Sequence<BulkRow> sequence = seq(rows, 2, ValueDesc.LONG, ValueDesc.FLOAT, ValueDesc.DOUBLE, ValueDesc.STRING);

    List<BulkRow> deserialized = serde(sequence);

    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(0), cr(1)}),
        GuavaUtils.arrayOfArrayToString(Lists.newArrayList(deserialized.get(0).decompose()).toArray(new Object[0][]))
    );
    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(2), cr(3)}),
        GuavaUtils.arrayOfArrayToString(Lists.newArrayList(deserialized.get(1).decompose()).toArray(new Object[0][]))
    );
    Assert.assertEquals(
        GuavaUtils.arrayOfArrayToString(new Object[][]{cr(4)}),
        GuavaUtils.arrayOfArrayToString(Lists.newArrayList(deserialized.get(2).decompose()).toArray(new Object[0][]))
    );
  }

  private final ObjectMapper mapper = new DefaultObjectMapper(new SmileFactory());

  private Sequence<BulkRow> seq(Sequence<Object[]> rows, int max, ValueDesc... types)
  {
    return new BulkSequence(rows, schema(types), 0, max, -1);
  }

  private List<BulkRow> serde(Sequence<BulkRow> sequence) throws IOException
  {
    return mapper.readValue(mapper.writeValueAsBytes(sequence), new TypeReference<List<Row>>() {});
  }

  private Sequence<BulkRow> seqSerde(Sequence<BulkRow> sequence) throws IOException
  {
    byte[] bytes = mapper.writeValueAsBytes(sequence);
    JsonParser jp = mapper.getFactory().createParser(bytes);
    Assert.assertEquals(JsonToken.START_ARRAY, jp.nextToken());
    Assert.assertEquals(JsonToken.START_OBJECT, jp.nextToken());
    return Sequences.once(new Iterator<BulkRow>()
    {
      private final ObjectCodec codec = jp.getCodec();

      @Override
      public boolean hasNext()
      {
        return !jp.isClosed();
      }

      @Override
      public BulkRow next()
      {
        try {
          final BulkRow retVal = codec.readValue(jp, BulkRow.class);
          if (jp.nextToken() == JsonToken.END_ARRAY) {
            CloseQuietly.close(jp);
          }
          return retVal;
        }
        catch (Throwable e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Test
  public void testBulkOnConcat() throws IOException
  {
    Sequence<Object[]> rows = Sequences.concat(
        Sequences.simple(cr(0), cr(1), cr(2), cr(3), cr(4)),
        Sequences.simple(cr(5), cr(6), cr(7)),
        Sequences.simple(cr(8), cr(9), cr(10), cr(11), cr(12))
    );
    Sequence<BulkRow> sequence = seq(rows, 5, ValueDesc.LONG, ValueDesc.FLOAT, ValueDesc.DOUBLE, ValueDesc.STRING);

    Yielder<BulkRow> yielder = Yielders.each(seqSerde(sequence));
    Assert.assertFalse(yielder.isDone());
    BulkRow g1 = yielder.get();
    Assert.assertEquals(5, g1.count());
    Assert.assertArrayEquals(new long[]{0L, 1L, 2L, 3L, 4L}, (long[]) g1.values()[0]);

    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    BulkRow g2 = yielder.get();
    Assert.assertEquals(5, g2.count());
    Assert.assertArrayEquals(new long[]{5L, 6L, 7L, 8L, 9L}, (long[]) g2.values()[0]);

    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    BulkRow g3 = yielder.get();
    Assert.assertEquals(3, g3.count());
    Assert.assertArrayEquals(new long[]{10L, 11L, 12L}, (long[]) g3.values()[0]);

    yielder = yielder.next(null);
    Assert.assertTrue(yielder.isDone());
  }

  @Test
  public void testBulkOnLimitOnConcat() throws IOException
  {
    Sequence<Object[]> rows = Sequences.concat(
        Sequences.simple(cr(0), cr(1), cr(2), cr(3), cr(4)),
        Sequences.simple(cr(5), cr(6), cr(7)),
        Sequences.simple(cr(8), cr(9), cr(10), cr(11), cr(12))
    );
    Sequence<BulkRow> sequence = seq(
        Sequences.limit(rows, 7), 3, ValueDesc.LONG, ValueDesc.FLOAT, ValueDesc.DOUBLE, ValueDesc.STRING
    );

    Yielder<BulkRow> yielder = Yielders.each(sequence);
    Assert.assertFalse(yielder.isDone());
    BulkRow g1 = yielder.get();
    Assert.assertEquals(3, g1.count());
    Assert.assertArrayEquals(new long[]{0L, 1L, 2L}, (long[]) g1.values()[0]);

    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    BulkRow g2 = yielder.get();
    Assert.assertEquals(3, g2.count());
    Assert.assertArrayEquals(new long[]{3L, 4L, 5L}, (long[]) g2.values()[0]);

    yielder = yielder.next(null);
    Assert.assertFalse(yielder.isDone());
    BulkRow g3 = yielder.get();
    Assert.assertEquals(1, g3.count());
    Assert.assertArrayEquals(new long[]{6L}, (long[]) g3.values()[0]);

    yielder = yielder.next(null);
    Assert.assertTrue(yielder.isDone());
  }

  private Object[] cr(long x)
  {
    return new Object[]{x, (float) x, (double) x, String.valueOf(x)};
  }
}
