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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.logger.Logger;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.query.RowBinding;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DataSchema
{
  private static final Logger log = new Logger(DataSchema.class);

  private final String dataSource;
  private final Map<String, Object> parser;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;

  private final List<Evaluation> evaluations;
  private final List<Validation> validations;
  private final ObjectMapper jsonMapper;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("evaluations") List<Evaluation> evaluations,
      @JsonProperty("validations") List<Validation> validations,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "null ObjectMapper.");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource cannot be null. Please provide a dataSource.");
    this.parser = parser;

    if (aggregators == null || aggregators.length == 0) {
      log.warn("No metricsSpec has been specified. Are you sure this is what you want?");
    }
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;

    if (granularitySpec == null) {
      log.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
      this.granularitySpec = new UniformGranularitySpec(null, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }
    this.evaluations = evaluations;
    this.validations = validations;
  }

  @VisibleForTesting
  public DataSchema(
      String dataSource,
      Map<String, Object> parser,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec,
      ObjectMapper jsonMapper
  )
  {
    this(dataSource, parser, aggregators, granularitySpec, null, null, jsonMapper);
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("parser")
  public Map<String, Object> getParserMap()
  {
    return parser;
  }

  @JsonIgnore
  public InputRowParser getParser()
  {
    if(parser == null) {
      log.warn("No parser has been specified");
      return null;
    }

    final InputRowParser parser = createInputRowParser();
    if ((evaluations == null || evaluations.isEmpty()) && (validations == null || validations.isEmpty())) {
      return parser;
    }

    final Map<String, ValueDesc> types = AggregatorFactory.toExpectedInputType(aggregators);
    final List<RowEvaluator<InputRow>> evaluators = Evaluation.toEvaluators(evaluations, types);
    final List<RowEvaluator<Boolean>> validators = Validation.toEvaluators(validations, types);
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
        InputRow inputRow = parser.parse(input);
        if (inputRow == null) {
          return null;
        }
        for (RowEvaluator<InputRow> evaluator : evaluators) {
          inputRow = evaluator.evaluate(inputRow);
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

  private InputRowParser createInputRowParser()
  {
    final InputRowParser inputRowParser = jsonMapper.convertValue(this.parser, InputRowParser.class);

    final Set<String> dimensionExclusions = Sets.newHashSet();
    for (AggregatorFactory aggregator : aggregators) {
      dimensionExclusions.addAll(aggregator.requiredFields());
      dimensionExclusions.add(aggregator.getName());
    }

    ParseSpec parseSpec = inputRowParser.getParseSpec();
    if (parseSpec != null) {
      final DimensionsSpec dimensionsSpec = parseSpec.getDimensionsSpec();
      final TimestampSpec timestampSpec = parseSpec.getTimestampSpec();

      // exclude timestamp from dimensions by default, unless explicitly included in the list of dimensions
      if (timestampSpec != null && timestampSpec.getTimestampColumn() != null) {
        final String timestampColumn = timestampSpec.getTimestampColumn();
        if (!(dimensionsSpec.hasCustomDimensions() && dimensionsSpec.getDimensionNames().contains(timestampColumn))) {
          dimensionExclusions.add(timestampColumn);
        }
      }
      if (dimensionsSpec != null) {
        final Set<String> metSet = Sets.newHashSet();
        for (AggregatorFactory aggregator : aggregators) {
          metSet.add(aggregator.getName());
        }
        final Set<String> dimSet = Sets.newHashSet(dimensionsSpec.getDimensionNames());
        final Set<String> overlap = Sets.intersection(metSet, dimSet);
        if (!overlap.isEmpty()) {
          throw new IAE(
              "Cannot have overlapping dimensions and metrics of the same name. Please change the name of the metric. Overlap: %s",
              overlap
          );
        }

        return inputRowParser.withParseSpec(
            parseSpec.withDimensionsSpec(
                dimensionsSpec.withDimensionExclusions(
                    Sets.difference(dimensionExclusions, dimSet)
                )
            )
        );
      } else {
        return inputRowParser;
      }
    } else {
      log.warn("No parseSpec in parser has been specified.");
      return inputRowParser;
    }
  }

  @JsonProperty("metricsSpec")
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty("evaluations")
  public List<Evaluation> getEvaluations()
  {
    return evaluations;
  }

  @JsonProperty("validations")
  public List<Validation> getValidations()
  {
    return validations;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  public DataSchema withDataSource(String dataSource)
  {
    return new DataSchema(dataSource, parser, aggregators, granularitySpec, evaluations, validations, jsonMapper);
  }

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    return new DataSchema(dataSource, parser, aggregators, granularitySpec, evaluations, validations, jsonMapper);
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", parser=" + parser +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", evaluations=" + evaluations +
           ", validations=" + validations +
           ", granularitySpec=" + granularitySpec +
           '}';
  }

  public static final class WithRecursion<T> extends RowBinding
  {
    private final String defaultColumn;
    private volatile boolean evaluated;
    private volatile T tempResult;

    public WithRecursion(String defaultColumn, TypeResolver types)
    {
      super(types);
      this.defaultColumn = defaultColumn;
    }

    public WithRecursion(String defaultColumn, Map<String, ValueDesc> types)
    {
      this(defaultColumn, new TypeResolver.WithMap(types));
    }

    public void set(T eval)
    {
      this.evaluated = true;
      this.tempResult = eval;
    }

    @Override
    public Object get(String name)
    {
      if (name.equals("_")) {
        return evaluated ? tempResult : super.get(defaultColumn);
      }
      return super.get(name);
    }

    @Override
    public void reset(Row row)
    {
      super.reset(row);
      this.evaluated = false;
      this.tempResult = null;
    }

    public T get()
    {
      return tempResult;
    }
  }
}
