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

package io.druid.data.input;

import io.druid.common.guava.GuavaUtils;
import io.druid.data.ParsingFail;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;
import java.util.Map;

/**
 */
public class InputRowParsers
{
  public static InputRowParser wrap(
      final InputRowParser parser,
      final AggregatorFactory[] aggregators,
      final List<Evaluation> evaluations,
      final List<Validation> validations
  )
  {
    if (GuavaUtils.isNullOrEmpty(evaluations) && GuavaUtils.isNullOrEmpty(validations)) {
      return parser;
    }

    final Map<String, ValueDesc> mapping = AggregatorFactory.toExpectedInputType(aggregators);
    for (DimensionSchema dimension : parser.getParseSpec().getDimensionsSpec().getDimensions()) {
      mapping.put(dimension.getName(), ValueDesc.ofDimension(dimension.getValueType()));
    }
    final TypeResolver resolver = new TypeResolver.WithMap(mapping);

    final List<RowEvaluator<InputRow>> evaluators = Evaluation.toEvaluators(evaluations, resolver);
    final List<RowEvaluator<Boolean>> validators = Validation.toEvaluators(validations, resolver);

    return new InputRowParser.Delegated()
    {
      @Override
      public InputRowParser getDelegate()
      {
        return parser;
      }

      @Override
      public InputRow parse(Object input)
      {
        @SuppressWarnings("unchecked")
        InputRow inputRow = parser.parse(input);
        if (inputRow == null) {
          return null;
        }
        for (RowEvaluator<InputRow> evaluator : evaluators) {
          try {
            inputRow = evaluator.evaluate(inputRow);
          }
          catch (Exception e) {
            throw ParsingFail.propagate(input, e);
          }
        }
        for (RowEvaluator<Boolean> validator : validators) {
          if (!validator.evaluate(inputRow)) {
            return null;
          }
        }
        return inputRow;
      }

      @Override
      public ParseSpec getParseSpec()
      {
        return parser.getParseSpec();
      }

      @Override
      public InputRowParser withParseSpec(ParseSpec parseSpec)
      {
        throw new UnsupportedOperationException("withParseSpec");
      }
    };
  }
}
