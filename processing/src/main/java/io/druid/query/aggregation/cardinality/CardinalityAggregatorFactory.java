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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import org.apache.commons.codec.binary.Base64;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class CardinalityAggregatorFactory extends AggregatorFactory implements AggregatorFactory.CubeSupport
{
  private static final byte CACHE_TYPE_ID = (byte) 0x8;

  private final String name;
  private final String predicate;
  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final GroupingSetSpec groupingSets;
  private final boolean byRow;
  private final boolean round;

  @JsonCreator
  public CardinalityAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("groupingSets") final GroupingSetSpec groupingSets,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("round") final boolean round
  )
  {
    this.name = name;
    this.predicate = predicate;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.byRow = byRow;
    this.round = round;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
  }

  public CardinalityAggregatorFactory(String name, List<String> fieldNames, boolean byRow)
  {
    this(name, fieldNames, null, null, null, byRow, false);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = makeDimensionSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = null;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher predicate = ColumnSelectors.toMatcher(this.predicate, columnFactory);
    return new CardinalityAggregator(predicate, selectors, grouping, byRow);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = makeDimensionSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = null;
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher predicate = ColumnSelectors.toMatcher(this.predicate, columnFactory);
    return new CardinalityBufferAggregator(selectors, predicate, grouping, byRow);
  }

  private List<DimensionSelector> makeDimensionSelectors(
      final List<DimensionSpec> dimensionSpecs,
      final ColumnSelectorFactory columnFactory
  )
  {
    return Lists.newArrayList(
        Lists.transform(
            Preconditions.checkNotNull(dimensionSpecs),
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
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner<HyperLogLogCollector>()
    {
      @Override
      public HyperLogLogCollector combine(HyperLogLogCollector param1, HyperLogLogCollector param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        return param1.fold(param2);
      }
    };
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
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }

    return HyperLogLogCollector.makeCollector(buffer);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return HyperUniquesAggregatorFactory.estimateCardinality(object, round);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return round ? ValueDesc.LONG : ValueDesc.DOUBLE;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public String getFieldName()
  {
    return fieldNames != null ? Iterables.getOnlyElement(fieldNames, null) : null;
  }

  @Override
  public AggregatorFactory getCombiningFactory(String inputField)
  {
    return new HyperUniquesAggregatorFactory(name, inputField, null, round);
  }

  @Override
  public String getCubeName()
  {
    return "cardinality";
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
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
      required.addAll(DimensionSpecs.toInputNames(fields));
    }
    return required;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
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
    return KeyBuilder.get()
                     .append(CACHE_TYPE_ID)
                     .append(fieldNames)
                     .append(fields)
                     .append(byRow, round)
                     .build();
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("hyperUnique");
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
    if (!Objects.equals(groupingSets, that.groupingSets)) {
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
           ", fields=" + fields +
           ", groupingSets=" + groupingSets +
           ", predicate='" + predicate + '\'' +
           ", byRow=" + byRow +
           ", round=" + round +
           '}';
  }
}
