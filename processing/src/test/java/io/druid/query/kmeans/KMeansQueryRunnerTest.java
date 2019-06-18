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

package io.druid.query.kmeans;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharSource;
import io.druid.common.Intervals;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.math.expr.Parser;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.DurationFieldType;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 */
public class KMeansQueryRunnerTest extends QueryRunnerTestHelper
{
  static final String KMEANS_DS = "kmeans_test";

  static {
    Parser.register(ModuleBuiltinFunctions.class);

    AggregatorFactory x = new RelayAggregatorFactory("x", ValueDesc.DOUBLE);
    AggregatorFactory y = new RelayAggregatorFactory("y", ValueDesc.DOUBLE);
    DimensionsSpec dimensions = new DimensionsSpec(
        StringDimensionSchema.ofNames("index"), null, null
    );
    IncrementalIndexSchema schema = TestIndex.SAMPLE_SCHEMA
        .withMinTimestamp(new DateTime("2011-01-01T00:00:00.000Z").getMillis())
        .withDimensionsSpec(dimensions)
        .withMetrics(x, y)
        .withRollup(false);

    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec("ts", "iso", null),
            dimensions,
            "\t",
            "\u0001",
            Arrays.asList("ts", "index", "x", "y")
        )
        , "utf8"
    );
    final Interval interval = Intervals.of("2018-07-09/2018-07-10");
    final Iterator<String> iterator = new Iterator<String>()
    {
      private final DateTime start = new DateTime("2018-07-09");
      private final int limit = 8000;
      private final Random x = new Random(0);
      private final Random x1 = new Random(0);
      private final Random x2 = new Random(0);
      private final Random y1 = new Random(0);
      private final Random y2 = new Random(0);
      private final StringBuilder b = new StringBuilder();

      private int index = 0;

      @Override
      public boolean hasNext()
      {
        return index < limit;
      }

      @Override
      public String next()
      {
        b.setLength(0);
        DateTime time = start.withFieldAdded(DurationFieldType.seconds(), index);
        b.append(time.getMillis()).append('\t').append(index++).append('\t');
        if (x.nextDouble() < 0.66) {
          b.append(x1.nextGaussian() * 600 + 1000).append('\t').append(y1.nextGaussian() * 300 + 500);
        } else {
          b.append(x2.nextGaussian() * 200 - 500).append('\t').append(y2.nextGaussian() * 100 - 300);
        }
        return b.append('\n').toString();
      }
    };
    final CharSource source = new CharSource()
    {
      @Override
      public Reader openStream() throws IOException
      {
        return new Reader()
        {
          private char[] current;
          private int offset;

          @Override
          public int read(char[] cbuf, int off, int len) throws IOException
          {
            while (current == null || offset >= current.length) {
              if (!iterator.hasNext()) {
                return -1;
              }
              current = iterator.next().toCharArray();
              offset = 0;
            }
            final int length = Math.min(len, current.length - offset);
            System.arraycopy(current, offset, cbuf, off, length);
            offset += length;
            return length;
          }

          @Override
          public void close() throws IOException
          {
          }
        };
      }
    };
    DataSegment segment = new DataSegment(
        KMEANS_DS,
        interval,
        "0",
        null,
        Arrays.asList("market", "market_month"),
        Arrays.asList("value"),
        null,
        null,
        0
    );
    TestIndex.segmentWalker.add(segment, TestIndex.makeRealtimeIndex(source, schema, parser));
  }

  @Test
  public void test()
  {
    KMeansQuery kMeans = new KMeansQuery(
        TableDataSource.of(KMEANS_DS),
        MultipleIntervalSegmentSpec.of(Intervals.of("2018-07-09/2018-07-10")),
        null,
        null,
        Arrays.asList("x", "y"),
        2,
        1000,
        0.0000001,
        null,
        ImmutableMap.<String, Object>of("$seed", 1)
    );
    List<Centroid> centroids = runQuery(kMeans);
    Collections.sort(centroids);

    double[] coord1 = centroids.get(1).getCentroid();
    double[] coord2 = centroids.get(0).getCentroid();

    Assert.assertArrayEquals(new double[]{1164.6461196729006, 582.3230598364503}, coord1, 1);
    Assert.assertArrayEquals(new double[]{-369.6083816855103, -223.72472630273845}, coord2, 1);
  }
}
