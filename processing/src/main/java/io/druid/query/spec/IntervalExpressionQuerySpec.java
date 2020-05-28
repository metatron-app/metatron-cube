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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.druid.common.guava.GuavaUtils;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Parser;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

@JsonTypeName("expression")
public class IntervalExpressionQuerySpec implements QuerySegmentSpec
{
  public static QuerySegmentSpec of(String... expressions)
  {
    return new IntervalExpressionQuerySpec(Arrays.asList(expressions));
  }

  private final List<String> expressions;

  @JsonCreator
  public IntervalExpressionQuerySpec(
      @JsonProperty("expressions") List<String> expressions
  )
  {
    Preconditions.checkArgument(!GuavaUtils.isNullOrEmpty(expressions), "expressions");
    this.expressions = expressions;
  }

  @JsonProperty
  public List<String> getExpressions()
  {
    return expressions;
  }

  @Override
  @JsonIgnore
  public List<Interval> getIntervals()
  {
    return ImmutableList.copyOf(
        Iterables.transform(expressions, expression -> (Interval) Evals.getConstant(Parser.parse(expression)))
    );
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForIntervals(query, getIntervals());
  }

  @Override
  public boolean equals(Object other)
  {
    return other instanceof IntervalExpressionQuerySpec &&
           expressions.equals(((IntervalExpressionQuerySpec) other).expressions);
  }
}
