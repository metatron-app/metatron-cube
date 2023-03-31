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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Predicate;
import io.druid.data.input.Row;
import io.druid.query.RowSignature;

/**
 * A "having" clause that filters aggregated/dimension value. This is similar to SQL's "having"
 * clause.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = ExpressionHavingSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "always", value = AlwaysHavingSpec.class),
    @JsonSubTypes.Type(name = "never", value = NeverHavingSpec.class),
    @JsonSubTypes.Type(name = "and", value = AndHavingSpec.class),
    @JsonSubTypes.Type(name = "equalTo", value = EqualToHavingSpec.class),
    @JsonSubTypes.Type(name = "greaterThan", value = GreaterThanHavingSpec.class),
    @JsonSubTypes.Type(name = "lessThan", value = LessThanHavingSpec.class),
    @JsonSubTypes.Type(name = "not", value = NotHavingSpec.class),
    @JsonSubTypes.Type(name = "or", value = OrHavingSpec.class),
    @JsonSubTypes.Type(name = "dimSelector", value = DimensionSelectorHavingSpec.class),
    @JsonSubTypes.Type(name = "expression", value = ExpressionHavingSpec.class),
    @JsonSubTypes.Type(name = "greaterThanOrEqualTo", value = GreaterThanOrEQHavingSpec.class),
    @JsonSubTypes.Type(name = "lessThanOrEqualTo", value = LessThanOrEQHavingSpec.class)
})
public interface HavingSpec
{
  // Atoms for easy combination, but for now they are mostly useful
  // for testing.
  HavingSpec NEVER = new NeverHavingSpec();
  HavingSpec ALWAYS = new AlwaysHavingSpec();

  /**
   * Evaluates if a given row satisfies the having spec.
   *
   * @return return evaluator
   * @param signature
   */
  Predicate<Row> toEvaluator(RowSignature signature);

  interface PostMergeSupport extends HavingSpec
  {
    Predicate<Row> toCompactEvaluator(RowSignature signature);
  }
}
