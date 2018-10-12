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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.common.Cacheable;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.DimensionSelector;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDimensionSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "default", value = DefaultDimensionSpec.class),
    @JsonSubTypes.Type(name = "extraction", value = ExtractionDimensionSpec.class),
    @JsonSubTypes.Type(name = "regexFiltered", value = RegexFilteredDimensionSpec.class),
    @JsonSubTypes.Type(name = "listFiltered", value = ListFilteredDimensionSpec.class),
    @JsonSubTypes.Type(name = "lookup", value = LookupDimensionSpec.class),
    @JsonSubTypes.Type(name = "expression", value = ExpressionDimensionSpec.class),
    @JsonSubTypes.Type(name = "withOrdering", value = DimensionSpecWithOrdering.class)
})
public interface DimensionSpec extends Cacheable, TypeResolver.Resolvable
{
  String getDimension();

  String getOutputName();

  //ExtractionFn can be implemented with decorate(..) fn
  ExtractionFn getExtractionFn();

  DimensionSelector decorate(DimensionSelector selector);

  DimensionSpec withOutputName(String outputName);

  boolean preservesOrdering();

  // for logging
  String getDescription();

  abstract class Abstract implements DimensionSpec
  {
    @Override
    public ValueDesc resolve(TypeResolver resolver)
    {
      // dimension : dimensions, __time, not-existing or virtual columns
      ValueDesc resolved = resolver.resolve(getDimension(), ValueDesc.STRING);
      return resolved.isUnknown() && getExtractionFn() != null ? ValueDesc.STRING : resolved;
    }
  }
}
