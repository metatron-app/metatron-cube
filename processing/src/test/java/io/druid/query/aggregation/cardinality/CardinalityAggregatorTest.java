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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.dimension.RegexFilteredDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CardinalityAggregatorTest
{
  public static class TestDimensionSelector implements DimensionSelector
  {
    private final List<Integer[]> column;
    private final Map<String, Integer> ids;
    private final Map<Integer, String> lookup;
    private final ExtractionFn exFn;

    private int pos = 0;

    public TestDimensionSelector(Iterable<String[]> values, ExtractionFn exFn)
    {
      this.lookup = Maps.newHashMap();
      this.ids = Maps.newHashMap();
      this.exFn = exFn;

      int index = 0;
      for (String[] multiValue : values) {
        for (String value : multiValue) {
          if (!ids.containsKey(value)) {
            ids.put(value, index);
            lookup.put(index, value);
            index++;
          }
        }
      }

      this.column = Lists.newArrayList(
          Iterables.transform(
              values, new Function<String[], Integer[]>()
              {
                @Nullable
                @Override
                public Integer[] apply(@Nullable String[] input)
                {
                  return Iterators.toArray(
                      Iterators.transform(
                          Iterators.forArray(input), new Function<String, Integer>()
                          {
                            @Nullable
                            @Override
                            public Integer apply(@Nullable String input)
                            {
                              return ids.get(input);
                            }
                          }
                      ), Integer.class
                  );
                }
              }
          )
      );
    }

    public void increment()
    {
      pos++;
    }

    public void reset()
    {
      pos = 0;
    }

    @Override
    public IndexedInts getRow()
    {
      final int p = this.pos;
      return new IndexedInts()
      {
        @Override
        public int size()
        {
          return column.get(p).length;
        }

        @Override
        public int get(int i)
        {
          return column.get(p)[i];
        }

        @Override
        public Iterator<Integer> iterator()
        {
          return Iterators.forArray(column.get(p));
        }

        @Override
        public void fill(int index, int[] toFill)
        {
          throw new UnsupportedOperationException("fill not supported");
        }

        @Override
        public void close() throws IOException
        {

        }
      };
    }

    @Override
    public int getValueCardinality()
    {
      return 1;
    }

    @Override
    public Comparable lookupName(int i)
    {
      String val = lookup.get(i);
      return exFn == null ? val : exFn.apply(val);
    }

    @Override
    public ValueDesc type()
    {
      return ValueDesc.STRING;
    }

    @Override
    public int lookupId(Comparable s)
    {
      return ids.get(s);
    }
  }

  /*
    values1: 4 distinct rows
    values1: 4 distinct values
    values2: 8 distinct rows
    values2: 7 distinct values
    groupBy(values1, values2): 9 distinct rows
    groupBy(values1, values2): 7 distinct values
    combine(values1, values2): 8 distinct rows
    combine(values1, values2): 7 distinct values
   */
  private static final List<String[]> values1 = dimensionValues(
      "a", "b", "c", "a", "a", null, "b", "b", "b", "b", "a", "a"
  );
  private static final List<String[]> values2 = dimensionValues(
      "a",
      "b",
      "c",
      "x",
      "a",
      "e",
      "b",
      new String[]{null, "x"},
      new String[]{"x", null},
      new String[]{"y", "x"},
      new String[]{"x", "y"},
      new String[]{"x", "y", "a"}
  );

  private static List<String[]> dimensionValues(Object... values)
  {
    return Lists.transform(
        Lists.newArrayList(values), new Function<Object, String[]>()
        {
          @Nullable
          @Override
          public String[] apply(@Nullable Object input)
          {
            if (input instanceof String[]) {
              return (String[]) input;
            } else {
              return new String[]{(String) input};
            }
          }
        }
    );
  }

  private static HyperLogLogCollector aggregate(List<DimensionSelector> selectorList, CardinalityAggregator agg, HyperLogLogCollector aggregate)
  {
    aggregate = agg.aggregate(aggregate);

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
    return aggregate;
  }

  private static void bufferAggregate(
      List<DimensionSelector> selectorList,
      BufferAggregator agg,
      ByteBuffer buf,
      int pos
  )
  {
    agg.aggregate(buf, pos);

    for (DimensionSelector selector : selectorList) {
      ((TestDimensionSelector) selector).increment();
    }
  }

  List<DimensionSelector> selectorList;
  CardinalityAggregatorFactory rowAggregatorFactory;
  CardinalityAggregatorFactory valueAggregatorFactory;
  final TestDimensionSelector dim1;
  final TestDimensionSelector dim2;

  final List<DimensionSelector> selectorListWithExtraction;
  final TestDimensionSelector dim1WithExtraction;
  final TestDimensionSelector dim2WithExtraction;

  final List<DimensionSelector> selectorListConstantVal;
  final TestDimensionSelector dim1ConstantVal;
  final TestDimensionSelector dim2ConstantVal;

  public CardinalityAggregatorTest()
  {
    dim1 = new TestDimensionSelector(values1, null);
    dim2 = new TestDimensionSelector(values2, null);

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1,
        dim2
    );

    rowAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );

    valueAggregatorFactory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1", "dim2"),
        true
    );


    String superJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn superFn = new JavaScriptExtractionFn(superJsFn, false, JavaScriptConfig.getDefault());
    dim1WithExtraction = new TestDimensionSelector(values1, superFn);
    dim2WithExtraction = new TestDimensionSelector(values2, superFn);
    selectorListWithExtraction = Lists.newArrayList(
        (DimensionSelector) dim1WithExtraction,
        dim2WithExtraction
    );

    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getDefault());
    dim1ConstantVal = new TestDimensionSelector(values1, helloFn);
    dim2ConstantVal = new TestDimensionSelector(values2, helloFn);
    selectorListConstantVal = Lists.newArrayList(
        (DimensionSelector) dim1ConstantVal,
        dim2ConstantVal
    );
  }

  @Test
  public void testAggregateRows() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        selectorList,
        true
    );

    HyperLogLogCollector aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorList, agg, aggregate);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(aggregate)), 0.05);
  }

  @Test
  public void testAggregateValues() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        selectorList,
        false
    );

    HyperLogLogCollector aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorList, agg, aggregate);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get(aggregate)), 0.05);
  }

  @Test
  public void testBufferAggregateRows() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
        true
    );

    int maxSize = rowAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
  }

  @Test
  public void testBufferAggregateValues() throws Exception
  {
    CardinalityBufferAggregator agg = new CardinalityBufferAggregator(
        selectorList,
        false
    );

    int maxSize = valueAggregatorFactory.getMaxIntermediateSize();
    ByteBuffer buf = ByteBuffer.allocate(maxSize + 64);
    int pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);

    for (int i = 0; i < values1.size(); ++i) {
      bufferAggregate(selectorList, agg, buf, pos);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get(buf, pos)), 0.05);
  }

  @Test
  public void testCombineRows()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    CardinalityAggregator agg1 = new CardinalityAggregator(selector1, true);
    CardinalityAggregator agg2 = new CardinalityAggregator(selector2, true);

    HyperLogLogCollector aggregate1 = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate1 = aggregate(selector1, agg1, aggregate1);
    }
    HyperLogLogCollector aggregate2 = null;
    for (int i = 0; i < values2.size(); ++i) {
      aggregate2 = aggregate(selector2, agg2, aggregate2);
    }

    Assert.assertEquals(4.0, (Double) rowAggregatorFactory.finalizeComputation(agg1.get(aggregate1)), 0.05);
    Assert.assertEquals(8.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get(aggregate2)), 0.05);

    Assert.assertEquals(
        9.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combiner().combine(
                agg1.get(aggregate1),
                agg2.get(aggregate2)
            )
        ),
        0.05
    );
  }

  @Test
  public void testCombineValues()
  {
    List<DimensionSelector> selector1 = Lists.newArrayList((DimensionSelector) dim1);
    List<DimensionSelector> selector2 = Lists.newArrayList((DimensionSelector) dim2);

    CardinalityAggregator agg1 = new CardinalityAggregator(selector1, false);
    CardinalityAggregator agg2 = new CardinalityAggregator(selector2, false);

    HyperLogLogCollector aggregate1 = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate1 = aggregate(selector1, agg1, aggregate1);
    }
    HyperLogLogCollector aggregate2 = null;
    for (int i = 0; i < values2.size(); ++i) {
      aggregate2 = aggregate(selector2, agg2, aggregate2);
    }

    Assert.assertEquals(4.0, (Double) valueAggregatorFactory.finalizeComputation(agg1.get(aggregate1)), 0.05);
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get(aggregate2)), 0.05);

    Assert.assertEquals(
        7.0,
        (Double) rowAggregatorFactory.finalizeComputation(
            rowAggregatorFactory.combiner().combine(
                agg1.get(aggregate1),
                agg2.get(aggregate2)
            )
        ),
        0.05
    );
  }

  @Test
  public void testAggregateRowsWithExtraction() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        selectorListWithExtraction,
        true
    );
    HyperLogLogCollector aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorListWithExtraction, agg, aggregate);
    }
    Assert.assertEquals(9.0, (Double) rowAggregatorFactory.finalizeComputation(agg.get(aggregate)), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        selectorListConstantVal,
        true
    );
    aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorListConstantVal, agg2, aggregate);
    }
    Assert.assertEquals(3.0, (Double) rowAggregatorFactory.finalizeComputation(agg2.get(aggregate)), 0.05);
  }

  @Test
  public void testAggregateValuesWithExtraction() throws Exception
  {
    CardinalityAggregator agg = new CardinalityAggregator(
        selectorListWithExtraction,
        false
    );
    HyperLogLogCollector aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorListWithExtraction, agg, aggregate);
    }
    Assert.assertEquals(7.0, (Double) valueAggregatorFactory.finalizeComputation(agg.get(aggregate)), 0.05);

    CardinalityAggregator agg2 = new CardinalityAggregator(
        selectorListConstantVal,
        false
    );
    aggregate = null;
    for (int i = 0; i < values1.size(); ++i) {
      aggregate = aggregate(selectorListConstantVal, agg2, aggregate);
    }
    Assert.assertEquals(1.0, (Double) valueAggregatorFactory.finalizeComputation(agg2.get(aggregate)), 0.05);
  }

  @Test
  public void testSerde() throws Exception
  {
    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        null,
        ImmutableList.<DimensionSpec>of(
            new DefaultDimensionSpec("b", "b"),
            new DefaultDimensionSpec("a", "a"),
            new DefaultDimensionSpec("c", "c")
        ),
        null,
        null,
        true,
        false
    );
    ObjectMapper objectMapper = new DefaultObjectMapper();
    Assert.assertEquals(
        factory,
        objectMapper.readValue(objectMapper.writeValueAsString(factory), AggregatorFactory.class)
    );

    String fieldNamesOnly = "{\"type\":\"cardinality\",\"name\":\"billy\",\"fields\":[\"b\",\"a\",\"c\"],\"byRow\":true}";
    Assert.assertEquals(
        factory,
        objectMapper.readValue(fieldNamesOnly, AggregatorFactory.class)
    );

    CardinalityAggregatorFactory factory2 = new CardinalityAggregatorFactory(
        "billy",
        null,
        ImmutableList.<DimensionSpec>of(
            new ExtractionDimensionSpec("b", "b", new RegexDimExtractionFn(".*", false, null)),
            new RegexFilteredDimensionSpec(new DefaultDimensionSpec("a", "a"), ".*"),
            new DefaultDimensionSpec("c", "c")
        ),
        null,
        null,
        false,
        true
    );

    Assert.assertEquals(
        factory2,
        objectMapper.readValue(objectMapper.writeValueAsString(factory2), AggregatorFactory.class)
    );
  }
}
