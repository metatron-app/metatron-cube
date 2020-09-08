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

package io.druid.query.aggregation.countmin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.GroupingSetSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@JsonTypeName("countMin")
public class CountMinAggregatorFactory extends AggregatorFactory
{
  private static final byte CACHE_TYPE_ID = 0x24;

  private final String name;
  private final List<String> fieldNames;
  private final List<DimensionSpec> fields;
  private final GroupingSetSpec groupingSets;
  private final String predicate;
  private final boolean byRow;
  private final int width;
  private final int depth;
  private final boolean combine;

  @JsonCreator
  public CountMinAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldNames") final List<String> fieldNames,
      @JsonProperty("fields") final List<DimensionSpec> fields,
      @JsonProperty("groupingSets") final GroupingSetSpec groupingSets,
      @JsonProperty("predicate") final String predicate,
      @JsonProperty("byRow") final boolean byRow,
      @JsonProperty("width") final int width,
      @JsonProperty("depth") final int depth,
      @JsonProperty("combine") final boolean combine
  )
  {
    this.name = name;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.predicate = predicate;
    this.byRow = byRow;
    this.width = width;
    this.depth = depth;
    this.combine = combine;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
    Preconditions.checkArgument(width > 0 && depth > 0);
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      return Aggregators.asAggregator(
          combiner(), columnFactory.<CountMinSketch>makeObjectColumnSelector(Iterables.getOnlyElement(fieldNames))
      );
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = new int[][]{};
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    ValueMatcher matcher = ColumnSelectors.toMatcher(predicate, columnFactory);
    return new CountMinAggregator(matcher, selectors, grouping, byRow, width, depth);
  }


  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    if (combine) {
      return CountMinBufferAggregator.combiner(
          columnFactory.<CountMinSketch>makeObjectColumnSelector(Iterables.getOnlyElement(fieldNames)), width, depth
      );
    }
    List<DimensionSpec> dimensionSpecs = fieldNames == null ? fields : DefaultDimensionSpec.toSpec(fieldNames);
    List<DimensionSelector> selectors = DimensionSpecs.toSelectors(dimensionSpecs, columnFactory);

    int[][] grouping = new int[][]{};
    if (groupingSets != null) {
      grouping = groupingSets.getGroupings(DimensionSpecs.toOutputNames(dimensionSpecs));
    }
    final ValueMatcher predicate = ColumnSelectors.toMatcher(this.predicate, columnFactory);
    return new CountMinBufferAggregator(predicate, selectors, grouping, byRow, width, depth);
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator<CountMinSketch>()
    {
      @Override
      public int compare(CountMinSketch lhs, CountMinSketch rhs)
      {
        return lhs.compareTo(rhs);
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<CountMinSketch> combiner()
  {
    return new Combiner<CountMinSketch>()
    {
      @Override
      public CountMinSketch combine(CountMinSketch param1, CountMinSketch param2)
      {
        if (param1 == null) {
          return param2;
        }
        if (param2 == null) {
          return param1;
        }
        return param1.merge(param2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new CountMinAggregatorFactory(name, Arrays.asList(name), null, null, null, false, width, depth, true);
  }

  @Override
  protected boolean isMergeable(AggregatorFactory other)
  {
    return false;
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof CountMinSketch) {
      return object;
    }
    final byte[] buffer;
    if (object instanceof byte[]) {
      buffer = (byte[]) object;
    } else if (object instanceof String) {
      buffer = StringUtils.decodeBase64((String) object);
    } else {
      throw new ISE("?? %s", object.getClass().getSimpleName());
    }
    return CountMinSketch.fromCompressedBytes(buffer);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

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
  public int getWidth()
  {
    return width;
  }

  @JsonProperty
  public int getDepth()
  {
    return depth;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(fieldNames)
                  .append(fields)
                  .append(groupingSets)
                  .append(predicate)
                  .append(byRow)
                  .append(width)
                  .append(depth);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.of("countMin");
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return (width * depth + 2) * Integer.BYTES;
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

    CountMinAggregatorFactory that = (CountMinAggregatorFactory) o;

    if (byRow != that.byRow) {
      return false;
    }
    if (width != that.width) {
      return false;
    }
    if (depth != that.depth) {
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
    result = 31 * result + Objects.hashCode(groupingSets);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + (byRow ? 1 : 0);
    result = 31 * result + width;
    result = 31 * result + depth;
    return result;
  }

  @Override
  public String toString()
  {
    return "CountMinAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldNames='" + fieldNames + '\'' +
           ", fields=" + fields +
           ", groupingSets=" + groupingSets +
           ", predicate='" + predicate + '\'' +
           ", byRow=" + byRow +
           ", width=" + width +
           ", depth=" + depth +
           '}';
  }
}
