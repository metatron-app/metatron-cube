/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.sql.calcite.aggregation;

import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.aggregation.post.MathPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.sql.calcite.expression.DruidExpression;

import java.util.Objects;

public class DimensionExpression
{
  private final String outputName;
  private final DruidExpression expression;
  private final ValueDesc outputType;

  public DimensionExpression(
      final String outputName,
      final DruidExpression expression,
      final ValueDesc outputType
  )
  {
    this.outputName = outputName;
    this.expression = expression;
    this.outputType = outputType;
  }

  public String getOutputName()
  {
    return outputName;
  }

  public DruidExpression getDruidExpression()
  {
    return expression;
  }

  public ValueDesc getOutputType()
  {
    return outputType;
  }

  public DimensionSpec toDimensionSpec(VColumnRegistry virtualColumns)
  {
    if (expression.isSimpleExtraction()) {
      return expression.getSimpleExtraction().toDimensionSpec(outputName);
    } else {
      return DefaultDimensionSpec.of(virtualColumns.register(outputName, expression).getOutputName(), outputName);
    }
  }

  public boolean isConstant()
  {
    return expression.isConstant();
  }

  public PostAggregator toPostAggregator()
  {
    if (expression.isSimpleExtraction()) {
      return new FieldAccessPostAggregator(outputName, expression.getDirectColumn());
    }
    return new MathPostAggregator(outputName, expression.getExpression());
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DimensionExpression that = (DimensionExpression) o;
    return Objects.equals(outputName, that.outputName) &&
           Objects.equals(expression, that.expression) &&
           outputType == that.outputType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(outputName, expression, outputType);
  }

  @Override
  public String toString()
  {
    return "DimensionExpression{" +
           "outputName='" + outputName + '\'' +
           ", expression=" + expression +
           ", outputType=" + outputType +
           '}';
  }
}
