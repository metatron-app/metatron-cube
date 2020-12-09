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

package io.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.cache.Cache;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.Sequence;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.LongMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.SelectQuery;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.ExprVirtualColumn;
import io.druid.segment.Metadata;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.StorageAdapterSegment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.ListIndexed;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ColumnExpanderTest
{
  public static class TestStorageAdapter implements StorageAdapter
  {
    private final List<String> dimensions;
    private final List<String> metrics = Lists.newArrayList();
    private final Map<String, ValueDesc> typesMap = Maps.newHashMap();
    private final List<ValueDesc> types = Lists.newArrayList();
    private final Metadata metadata = new Metadata();

    public TestStorageAdapter(List<String> dimensions, List<AggregatorFactory> aggregators)
    {
      this.dimensions = dimensions;
      for (String dimension : dimensions) {
        typesMap.put(dimension, ValueDesc.ofDimension(ValueType.STRING));
        types.add(ValueDesc.ofDimension(ValueType.STRING));
      }
      for (AggregatorFactory factory : aggregators) {
        typesMap.put(factory.getName(), factory.getOutputType());
        types.add(factory.getOutputType());
        metrics.add(factory.getName());
      }
      metadata.setAggregators(aggregators.toArray(new AggregatorFactory[0]));
    }

    @Override
    public String getSegmentIdentifier()
    {
      return null;
    }

    @Override
    public Interval getInterval()
    {
      return null;
    }

    @Override
    public Interval getTimeMinMax()
    {
      return null;
    }

    @Override
    public Indexed<String> getAvailableDimensions()
    {
      return new ListIndexed<String>(dimensions, String.class);
    }

    @Override
    public Iterable<String> getAvailableMetrics()
    {
      return metrics;
    }

    @Override
    public int getDimensionCardinality(String column)
    {
      return -1;
    }

    @Override
    public Capabilities getCapabilities()
    {
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      ValueDesc desc = typesMap.get(column);
      return desc == null ? null :
             ValueDesc.isDimension(desc) ? ColumnCapabilities.of(ValueDesc.typeOfDimension(desc)) :
             ColumnCapabilities.of(desc.type());
    }

    @Override
    public Map<String, String> getColumnDescriptor(String column)
    {
      return null;
    }

    @Override
    public long getSerializedSize(String column)
    {
      return 0;
    }

    @Override
    public float getAverageSize(String column)
    {
      return 0;
    }

    @Override
    public int getNumRows()
    {
      return 0;
    }

    @Override
    public Metadata getMetadata()
    {
      return metadata;
    }

    @Override
    public Schema asSchema(boolean prependTime)
    {
      List<String> columnNames = Lists.newArrayList();
      List<ValueDesc> columnTypes = Lists.newArrayList();
      if (prependTime) {
        columnNames.add(Row.TIME_COLUMN_NAME);
        columnTypes.add(ValueDesc.LONG);
      }
      columnNames.addAll(GuavaUtils.concat(dimensions, metrics));
      columnTypes.addAll(types);
      return new Schema(columnNames, columnTypes, AggregatorFactory.asMap(metadata.getAggregators()), null, null);
    }

    @Override
    public Sequence<Cursor> makeCursors(
        DimFilter filter,
        Interval interval,
        RowResolver resolver,
        Granularity granularity,
        boolean descending,
        Cache cache
    )
    {
      return null;
    }
  }

  private final AggregatorFactory met1 = new DoubleMinAggregatorFactory("met1", "");
  private final AggregatorFactory met2 = new LongSumAggregatorFactory("met2", "");
  private final AggregatorFactory met3 = new LongMinAggregatorFactory("met3", "");

  private final StorageAdapter adapter = new TestStorageAdapter(
      Arrays.asList("dim1", "dim2"),
      Arrays.asList(met1, met2)
  );
  private final VirtualColumn vc1 = new ExprVirtualColumn("met1 + met2", "vc1");
  private final VirtualColumn vc2 = new ExprVirtualColumn("met1 + met3", "vc2");
  private final VirtualColumn vc3 = new ExprVirtualColumn("met3 + met4", "vc3");

  @Test
  public void testRowResolverMerge()
  {
    Segment segment1 = new StorageAdapterSegment(adapter);
    Segment segment2 = new StorageAdapterSegment(
        new TestStorageAdapter(Arrays.asList("dim1", "dim3"), Arrays.asList(met1, met2, met3))
    );
    RowResolver merged = RowResolver.of(Arrays.asList(segment1, segment2), ImmutableList.<VirtualColumn>of());
    Assert.assertEquals(Arrays.asList("dim1", "dim2", "dim3"), merged.getDimensionNames());
    Assert.assertEquals(Arrays.asList("__time", "met1", "met2", "met3"), merged.getMetricNames());
  }

  @Test
  @Ignore
  public void testBasicOverrideGroupBy()
  {
    ViewDataSource view = new ViewDataSource(
        "test",
        Arrays.asList("dim1", "met1", "met2", "vc1", "vc2", "vc3"),
        Arrays.<VirtualColumn>asList(vc1, vc2),
        null,
        false
    );

    // allDimensionsForEmpty = false
    GroupByQuery base = new GroupByQuery.Builder()
        .setDataSource(view)
        .setGranularity(Granularities.ALL)
        .build();

    RowResolver resolver = RowResolver.of(adapter, view.getVirtualColumns());
    GroupByQuery x = (GroupByQuery) ColumnExpander.expand(base, resolver);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // allDimensionsForEmpty = true
    GroupByQuery q0 = base.withOverriddenContext(
        ImmutableMap.<String, Object>of(QueryContextKeys.ALL_DIMENSIONS_FOR_EMPTY, true)
    );
    RowResolver r0 = RowResolver.of(adapter, q0.getVirtualColumns());

    x = (GroupByQuery) ColumnExpander.expand(q0, r0);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // override dimension
    GroupByQuery q1 = q0.withDimensionSpecs(DefaultDimensionSpec.toSpec("dim2"));
    RowResolver r1 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q1));

    x = (GroupByQuery) ColumnExpander.expand(q1, r1);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim2"), x.getDimensions());  // do not modify
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());

    // allMetricsForEmpty = true
    GroupByQuery q2 = base.withOverriddenContext(
        ImmutableMap.<String, Object>of(QueryContextKeys.ALL_METRICS_FOR_EMPTY, true)
    );
    RowResolver r2 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q2));

    x = (GroupByQuery) ColumnExpander.expand(q2, r2);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(met1, met2), x.getAggregatorSpecs());

    // override metric
    GroupByQuery q3 = q2.withAggregatorSpecs(Arrays.<AggregatorFactory>asList(met3));
    RowResolver r3 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q3));

    x = (GroupByQuery) ColumnExpander.expand(q3, r3);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());  // dimension from segment
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(met3), x.getAggregatorSpecs());

    // override vc
    GroupByQuery q4 = base.withVirtualColumns(Arrays.<VirtualColumn>asList(vc3));
    RowResolver r4 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q4));

    x = (GroupByQuery) ColumnExpander.expand(q4, r4);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2, vc3), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec(), x.getDimensions());
    Assert.assertEquals(Arrays.<AggregatorFactory>asList(), x.getAggregatorSpecs());
  }

  @Test
  @Ignore
  public void testBasicOverrideSelect()
  {
    ViewDataSource view = new ViewDataSource(
        "test",
        Arrays.asList("dim1", "met1", "vc1", "vc2", "vc3"),
        Arrays.<VirtualColumn>asList(vc1, vc2),
        null,
        false
    );

    SelectQuery base = new Druids.SelectQueryBuilder()
        .dataSource(view)
        .granularity(Granularities.ALL)
        .build();
    RowResolver resolver = RowResolver.of(adapter, BaseQuery.getVirtualColumns(base));

    SelectQuery x = (SelectQuery) ColumnExpander.expand(base, resolver);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2"), x.getMetrics());

    // override dimension
    SelectQuery q1 = base.withDimensionSpecs(DefaultDimensionSpec.toSpec("dim2"));
    RowResolver r1 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q1));

    x = (SelectQuery) ColumnExpander.expand(q1, r1);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim2"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2"), x.getMetrics());

    // override vc
    SelectQuery q2 = base.withVirtualColumns(Arrays.<VirtualColumn>asList(vc3));
    RowResolver r2 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q2));

    x = (SelectQuery) ColumnExpander.expand(q2, r2);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2, vc3), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met1", "vc1", "vc2", "vc3"), x.getMetrics());

    SelectQuery q3 = base.withMetrics(Arrays.<String>asList("met2"));
    RowResolver r3 = RowResolver.of(adapter, BaseQuery.getVirtualColumns(q3));

    x = (SelectQuery) ColumnExpander.expand(q3, r3);
    Assert.assertTrue(x.getDataSource() instanceof TableDataSource);
    Assert.assertEquals(Arrays.<VirtualColumn>asList(vc1, vc2), x.getVirtualColumns());
    Assert.assertEquals(DefaultDimensionSpec.toSpec("dim1"), x.getDimensions());
    Assert.assertEquals(Arrays.asList("met2", "vc1", "vc2"), x.getMetrics());
  }
}
