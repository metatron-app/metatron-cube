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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupingSetSpec;

import java.util.List;
import java.util.Objects;

public abstract class HashAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  protected final String predicate;
  protected final List<String> fieldNames;
  protected final List<DimensionSpec> fields;
  protected final GroupingSetSpec groupingSets;
  protected final boolean byRow;

  public HashAggregatorFactory(
      String name,
      String predicate,
      List<String> fieldNames,
      List<DimensionSpec> fields,
      GroupingSetSpec groupingSets,
      boolean byRow
  )
  {
    this.name = name;
    this.predicate = predicate;
    this.fieldNames = fieldNames;
    this.fields = fields;
    this.groupingSets = groupingSets;
    this.byRow = byRow;
    Preconditions.checkArgument(
        fieldNames == null ^ fields == null,
        "Must have a valid, non-null fieldNames or fields"
    );
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

  @JsonIgnore
  public String getScannableColumn()
  {
    if (predicate != null) {
      return null;
    }
    if (fieldNames != null && fieldNames.size() == 1) {
      return fieldNames.get(0);
    }
    if (fields != null && fields.size() == 1) {
      return fields.get(0) instanceof DefaultDimensionSpec ? fields.get(0).getDimension() : null;
    }
    return null;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public GroupingSetSpec getGroupingSets()
  {
    return groupingSets;
  }

  @JsonProperty
  public boolean isByRow()
  {
    return byRow;
  }

  @Override
  protected boolean isMergeable(AggregatorFactory other)
  {
    return false;
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

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(fieldNames)
                  .append(fields)
                  .append(groupingSets)
                  .append(predicate)
                  .append(byRow);
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

    HashAggregatorFactory that = (HashAggregatorFactory) o;

    if (byRow != that.byRow) {
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
    result = 31 * result + Objects.hashCode(fieldNames);
    result = 31 * result + Objects.hashCode(fields);
    result = 31 * result + Objects.hashCode(predicate);
    result = 31 * result + (byRow ? 1 : 0);
    return result;
  }
}
