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

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.ParsingFail;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class InputRowParsers
{
  public static <T> InputRowParser<T> wrap(
      final InputRowParser<T> parser,
      final AggregatorFactory[] aggregators,
      final List<Evaluation> evaluations,
      final List<Validation> validations
  )
  {
    if (GuavaUtils.isNullOrEmpty(evaluations) && GuavaUtils.isNullOrEmpty(validations)) {
      return parser;
    }

    final Map<String, ValueDesc> mapping = AggregatorFactory.toExpectedInputType(aggregators);
    for (DimensionSchema dimension : parser.getDimensionsSpec().getDimensions()) {
      mapping.put(dimension.getName(), ValueDesc.ofDimension(dimension.getValueType()));
    }
    for (Evaluation evaluation : evaluations) {
      mapping.remove(evaluation.getOutputName());
    }
    final TypeResolver resolver = new TypeResolver.WithMap(mapping);
    final List<RowEvaluator<InputRow>> evaluators = Evaluation.toEvaluators(evaluations, resolver);
    final List<RowEvaluator<Boolean>> validators = Validation.toEvaluators(validations, resolver);

    return new InputRowParser.Delegated<T>()
    {
      @Override
      public boolean accept(Object input)
      {
        if (parser instanceof Streaming) {
          return ((Streaming<T>) parser).accept(input);
        }
        return false;
      }

      @Override
      public Iterator<InputRow> parseStream(Object input)
      {
        if (parser instanceof Streaming) {
          return Iterators.filter(
              Iterators.transform(
                  ((Streaming<T>) parser).parseStream(input),
                  new Function<InputRow, InputRow>()
                  {
                    @Override
                    public InputRow apply(InputRow input)
                    {
                      return convert(input);
                    }
                  }
              ),
              Predicates.<InputRow>notNull()
          );
        }
        throw new IllegalStateException();
      }

      @Override
      public InputRowParser<T> getDelegate()
      {
        return parser;
      }

      @Override
      public InputRow parse(T input)
      {
        return convert(parser.parse(input));
      }

      private InputRow convert(InputRow inputRow)
      {
        if (inputRow == null) {
          return null;
        }
        for (RowEvaluator<InputRow> evaluator : evaluators) {
          try {
            inputRow = evaluator.evaluate(inputRow);
          }
          catch (Exception e) {
            throw ParsingFail.propagate(inputRow, e);
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
      public TimestampSpec getTimestampSpec()
      {
        return parser.getTimestampSpec();
      }

      @Override
      public DimensionsSpec getDimensionsSpec()
      {
        return parser.getDimensionsSpec();
      }

      @Override
      public InputRowParser withDimensionExclusions(Set<String> exclusions)
      {
        throw new UnsupportedOperationException("withDimensionExclusions");
      }
    };
  }

  public static <T> Function<T, InputRow> asFunction(final InputRowParser<T> parser, final boolean ignoreInvalidRows)
  {
    return new Function<T, InputRow>()
    {
      @Override
      public InputRow apply(T input)
      {
        try {
          return parser.parse(input);
        }
        catch (Exception e) {
          if (!ignoreInvalidRows) {
            throw ParsingFail.propagate(input, e);
          }
          return null;
        }
      }
    };
  }
}
