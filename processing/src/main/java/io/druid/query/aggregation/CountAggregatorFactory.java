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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.filter.ValueMatchers;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.Cursor;
import io.druid.segment.ScanContext;
import io.druid.segment.Segment;
import io.druid.segment.column.ColumnMeta;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
public class CountAggregatorFactory extends AggregatorFactory
    implements AggregatorFactory.CubeSupport, AggregatorFactory.Vectorizable
{
  public static CountAggregatorFactory of(String name)
  {
    return new CountAggregatorFactory(name);
  }

  public static CountAggregatorFactory of(String name, String fieldName)
  {
    return new CountAggregatorFactory(name, fieldName, null);
  }

  public static CountAggregatorFactory predicate(String name, String predicate)
  {
    return new CountAggregatorFactory(name, null, predicate);
  }

  private static final byte[] CACHE_KEY = new byte[]{0x0};

  private final String name;
  private final String fieldName;
  private final String predicate;

  @JsonCreator
  public CountAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("predicate") String predicate
  )
  {
    this.name = name == null ? fieldName : name;
    this.fieldName = fieldName;
    this.predicate = predicate;
    Preconditions.checkNotNull(this.name, "Must have a valid, non-null aggregator name");
  }

  public CountAggregatorFactory(String name)
  {
    this(name, null, null);
  }

  @Override
  public AggregatorFactory optimize(Segment segment)
  {
    if (fieldName != null && predicate == null) {
      ColumnMeta meta = segment.asStorageAdapter(false).getColumnMeta(fieldName);
      if (meta != null && GuavaUtils.isFalse(meta.hasNull())) {
        return CountAggregatorFactory.of(name);
      }
    }
    return this;
  }

  @Override
  public AggregatorFactory evaluate(Cursor cursor, ScanContext context)
  {
    if (predicate != null) {
      return this;
    }
    if (fieldName == null) {
      return AggregatorFactory.constant(this, context.count());
    }
    ColumnMeta meta = cursor.getMeta(fieldName);
    if (meta != null) {
      ImmutableBitmap nulls = meta.getNulls();
      if (nulls != null) {
        return AggregatorFactory.constant(this, (long) context.count() - nulls.size());
      }
    }
    return this;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new CountAggregator(toValueMatcher(metricFactory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new CountBufferAggregator(toValueMatcher(metricFactory));
  }

  private ValueMatcher toValueMatcher(ColumnSelectorFactory metricFactory)
  {
    ValueMatcher matcher = null;
    if (fieldName != null) {
      matcher = ValueMatchers.not(metricFactory.makePredicateMatcher(DimFilters.isNull(fieldName)));
    }
    if (predicate != null) {
      matcher = ValueMatchers.and(matcher, ColumnSelectors.toMatcher(predicate, metricFactory));
    }
    return matcher == null ? ValueMatcher.TRUE : matcher;
  }

  @Override
  public Comparator getComparator()
  {
    return CountAggregator.COMPARATOR;
  }

  @Override
  public BinaryFn.Identical<Number> combiner()
  {
    return LongSumAggregator.COMBINER;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new LongSumAggregatorFactory(name, inputField);
  }

  @Override
  public String getCubeName()
  {
    return "count";
  }

  @Override
  public List<String> requiredFields()
  {
    return ImmutableList.of();
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_KEY)
                  .append(fieldName, predicate);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.LONG;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Long.BYTES;
  }

  @Override
  public String toString()
  {
    return "CountAggregatorFactory{" +
           "name='" + name + '\'' +
           (fieldName == null ? "" : ", fieldName='" + fieldName + '\'') +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           '}';
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

    CountAggregatorFactory that = (CountAggregatorFactory) o;

    if (!(Objects.equals(name, that.name))) {
      return false;
    }
    if (!(Objects.equals(fieldName, that.fieldName))) {
      return false;
    }
    if (!(Objects.equals(predicate, that.predicate))) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, predicate);
  }

  @Override
  public boolean supports(ColumnSelectorFactory factory)
  {
    return fieldName == null && predicate == null;
  }

  @Override
  public Aggregator.Vectorized create(ColumnSelectorFactory factory)
  {
    return CountAggregator.create();
  }
}
