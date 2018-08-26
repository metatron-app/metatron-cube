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

package io.druid.query.aggregation.cardinality;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.utils.StringUtils;
import io.druid.math.expr.Parser;
import io.druid.query.QueryCacheHelper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class CardinalityAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = (byte) 0x8;

  private final String name;
  private final String predicate;
  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final boolean byRow;
  private final boolean round;

  @JsonCreator
  public CardinalityAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("round") final boolean round
  )
  {
    this.name = name;
    this.predicate = predicate;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.byRow = byRow;
    this.round = round;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
  }

  public CardinalityAggregatorFactory(String name, List<String> fieldNames, boolean byRow)
  {
    this(name, fieldNames, null, null, byRow, false);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    List<DimensionSelector> selectors = makeDimensionSelectors(columnFactory);

    if (selectors.isEmpty()) {
      return Aggregators.noopAggregator();
    }

    return new CardinalityAggregator(ColumnSelectors.toMatcher(predicate, columnFactory), selectors, byRow);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    List<DimensionSelector> selectors = makeDimensionSelectors(columnFactory);

    if (selectors.isEmpty()) {
      return Aggregators.noopBufferAggregator();
    }

    return new CardinalityBufferAggregator(selectors, ColumnSelectors.toMatcher(predicate, columnFactory), byRow);
  }

  private List<DimensionSelector> makeDimensionSelectors(final ColumnSelectorFactory columnFactory)
  {
    return Lists.newArrayList(
        Lists.transform(
            Preconditions.checkNotNull(fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames)),
            new Function<DimensionSpec, DimensionSelector>()
            {
              @Override
              public DimensionSelector apply(DimensionSpec input)
              {
                return columnFactory.makeDimensionSelector(input);
              }
            }
        )
    );
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<HyperLogLogCollector>()
    {
      @Override
      public int compare(HyperLogLogCollector lhs, HyperLogLogCollector rhs)
      {
        return lhs.compareTo(rhs);
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return ((HyperLogLogCollector) lhs).fold((HyperLogLogCollector) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new HyperUniquesAggregatorFactory(name, name, null, round);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    throw new UnsupportedOperationException("can't merge CardinalityAggregatorFactory");
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return HyperLogLogCollector.makeCollector(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return HyperLogLogCollector.makeCollector((ByteBuffer) object);
    } else if (object instanceof String) {
      return HyperLogLogCollector.makeCollector(
          ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)))
      );
    }
    return object;
  }

  @Override

  public Object finalizeComputation(Object object)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(object, round);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getPredicate()
  {
    return predicate;
  }

  @Override
  public List<String> requiredFields()
  {
    List<String> required = Lists.newArrayList();
    if (fieldNames != null) {
      required.addAll(fieldNames);
    } else {
      required.addAll(Lists.transform(fields, DimensionSpecs.INPUT_NAME));
    }
    if (predicate != null) {
      required.addAll(Parser.findRequiredBindings(predicate));
    }
    return required;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public List<DimensionSpec> getFields()
  {
    return fields;
  }

  @JsonProperty
  public boolean isByRow()
  {
    return byRow;
  }

  @JsonProperty
  public boolean isRound()
  {
    return round;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldBytes;
    if (fieldNames != null) {
      fieldBytes = QueryCacheHelper.computeCacheBytes(fieldNames);
    } else {
      fieldBytes = QueryCacheHelper.computeCacheKeys(fields);
    }
    byte[] predicateBytes = StringUtils.toUtf8WithNullToEmpty(predicate);

    return ByteBuffer.allocate(3 + fieldBytes.length + predicateBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(fieldBytes)
                     .put(predicateBytes)
                     .put((byte) (byRow ? 1 : 0))
                     .put((byte) (round ? 1 : 0))
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "hyperUnique";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return HyperLogLogCollector.getLatestNumBytesForDenseStorage();
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

    CardinalityAggregatorFactory that = (CardinalityAggregatorFactory) o;

    if (byRow != that.byRow) {
      return false;
    }
    if (round != that.round) {
      return false;
    }
    if (!Objects.equals(fieldNames, that.fieldNames)) {
      return false;
    }
    if (!Objects.equals(fields, that.fields)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(predicate, that.predicate)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (fieldNames != null ? fieldNames.hashCode() : 0);
    result = 31 * result + (fields != null ? fields.hashCode() : 0);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + (byRow ? 1 : 0);
    result = 31 * result + (round ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "CardinalityAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames='" + fieldNames + '\'' +
           ", fields='" + fields + '\'' +
           ", predicate='" + predicate + '\'' +
           ", byRow=" + byRow +
           ", round=" + round +
           '}';
  }
}
