/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.granularity.Granularity;
import io.druid.granularity.QueryGranularities;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.RowResolver;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.Cursor;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class SelectMetaQueryEngine
{
  public Sequence<Result<SelectMetaResultValue>> process(final SelectMetaQuery query, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter(false);

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final Interval interval = Iterables.getOnlyElement(intervals);
    final Granularity granularity = query.getGranularity();

    final String segmentId = segment.getIdentifier();
    final StorageAdapter storageAdapter = segment.asStorageAdapter(false);

    // use segment resolver
    final RowResolver resolver = RowResolver.of(segment, VirtualColumns.valueOf(query.getVirtualColumns()));
    final Schema schema = makeSchema(query, resolver);

    if (query.isSchemaOnly()) {
      return Sequences.simple(
          Arrays.asList(
              new Result<SelectMetaResultValue>(
                  granularity.toDateTime(interval.getStartMillis()),
                  new SelectMetaResultValue(schema)
              )
          )
      );
    }

    final PagingOffset offset = query.getPagingOffset(segmentId);
    final float averageSize = calculateAverageSize(query, adapter);

    // minor optimization.. todo: we can do this even with filters set
    if (query.getDimensionsFilter() == null &&
        QueryGranularities.ALL.equals(granularity) &&
        interval.equals(segment.getDataInterval()) &&
        offset.startDelta() == 0) {
      int row = storageAdapter.getNumRows();
      return BaseSequence.simple(
          Arrays.asList(
              new Result<>(
                  granularity.toDateTime(interval.getStartMillis()),
                  new SelectMetaResultValue(
                      schema, ImmutableMap.of(segmentId, row), (long) (row * averageSize)
                  )
              )
          )
      );
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        intervals,
        resolver,
        query.getDimensionsFilter(),
        null,
        query.isDescending(),
        granularity,
        new Function<Cursor, Result<SelectMetaResultValue>>()
        {
          @Override
          public Result<SelectMetaResultValue> apply(Cursor cursor)
          {
            int row = 0;
            for (cursor.advanceTo(offset.startDelta()); !cursor.isDone(); cursor.advance()) {
              row++;
            }
            return new Result<>(
                cursor.getTime(),
                new SelectMetaResultValue(schema, ImmutableMap.of(segmentId, row), (long) (row * averageSize))
            );
          }
        }
    );
  }

  // not include virtual columns & not consider extract, lookup, etc.
  private float calculateAverageSize(SelectMetaQuery query, StorageAdapter adapter)
  {
    float averageSize = 0;
    final Set<String> dimensions = Sets.newHashSet(DimensionSpecs.toOutputNames(query.getDimensions()));
    for (String dimension : adapter.getAvailableDimensions()) {
      if (dimensions.contains(dimension)) {
        averageSize += adapter.getAverageSize(dimension);
      }
    }
    final Set<String> metrics = Sets.newHashSet(query.getMetrics());
    for (String metric : adapter.getAvailableMetrics()) {
      if (metrics == null || metrics.contains(metric)) {
        averageSize += adapter.getAverageSize(metric);
      }
    }
    return averageSize;
  }

  private Schema makeSchema(SelectMetaQuery query, RowResolver resolver)
  {
    if (GuavaUtils.isNullOrEmpty(query.getDimensions()) && GuavaUtils.isNullOrEmpty(query.getMetrics())) {
      return Schema.from(resolver).appendTime();
    }
    final List<String> dimensions = DimensionSpecs.toOutputNames(query.getDimensions());
    final List<String> metrics = Lists.newArrayList(query.getMetrics());

    final List<ValueDesc> columnTypes = Lists.newArrayList();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      if (dimensionSpec.getExtractionFn() == null) {
        columnTypes.add(dimensionSpec.resolveType(resolver));
      } else {
        columnTypes.add(ValueDesc.STRING);
      }
    }
    for (String metric : metrics) {
      columnTypes.add(resolver.resolveColumn(metric, ValueDesc.UNKNOWN));
    }
    List<AggregatorFactory> aggregators = Lists.newArrayList();
    Map<String, AggregatorFactory> factoryMap = resolver.getAggregators();
    for (String metric : metrics) {
      aggregators.add(factoryMap.get(metric));
    }

    return new Schema(dimensions, metrics, columnTypes, aggregators, resolver.getDescriptors());
  }
}
