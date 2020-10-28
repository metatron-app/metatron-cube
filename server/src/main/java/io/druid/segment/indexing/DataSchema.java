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

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.druid.data.input.Evaluation;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.Validation;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Parser;
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
  public static final String REQUIRED_COLUMNS = "required.columns";
  public static final String HADOOP_REQUIRED_COLUMNS = "hadoop.required.columns";

  private static final Logger LOG = new Logger(DataSchema.class);

  private final String dataSource;
  private final Map<String, Object> parser;
  private final AggregatorFactory[] aggregators;
  private final GranularitySpec granularitySpec;
  private final boolean enforceType;

  private final List<Evaluation> evaluations;
  private final List<Validation> validations;
  private final boolean dimensionFixed;

  @JsonCreator
  public DataSchema(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("metricsSpec") AggregatorFactory[] aggregators,
      @JsonProperty("enforceType") boolean enforceType,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      @JsonProperty("evaluations") List<Evaluation> evaluations,
      @JsonProperty("validations") List<Validation> validations,
      @JsonProperty("dimensionFixed") boolean dimensionFixed
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource cannot be null. Please provide a dataSource.");
    this.parser = parser;

    if (aggregators == null || aggregators.length == 0) {
      LOG.warn("No metricsSpec has been specified. Are you sure this is what you want?");
    }
    this.aggregators = aggregators == null ? new AggregatorFactory[0] : aggregators;
    this.enforceType = enforceType;

    if (granularitySpec == null) {
      LOG.warn("No granularitySpec has been specified. Using UniformGranularitySpec as default.");
      this.granularitySpec = new UniformGranularitySpec(null, null, null);
    } else {
      this.granularitySpec = granularitySpec;
    }
    this.evaluations = evaluations == null ? ImmutableList.<Evaluation>of() : evaluations;
    this.validations = validations == null ? ImmutableList.<Validation>of() : validations;
    this.dimensionFixed = dimensionFixed;
  }

  @VisibleForTesting
  public DataSchema(
      String dataSource,
      Map<String, Object> parser,
      AggregatorFactory[] aggregators,
      GranularitySpec granularitySpec
  )
  {
    this(dataSource, parser, aggregators, false, granularitySpec, null, null, false);
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

  public InputRowParser getParser(ObjectMapper mapper, boolean ignoreInvalidRows)
  {
    if (parser == null) {
      LOG.warn("No parser has been specified");
      return null;
    }
    final InputRowParser parser = createInputRowParser(mapper, ignoreInvalidRows);
    return InputRowParsers.wrap(parser, aggregators, evaluations, validations, enforceType, ignoreInvalidRows);
  }

  private InputRowParser createInputRowParser(ObjectMapper mapper, boolean ignoreInvalidRows)
  {
    InputRowParser inputRowParser = mapper.convertValue(parser, InputRowParser.class);
    if (inputRowParser instanceof InputRowParser.Streaming) {
      inputRowParser = ((InputRowParser.Streaming) inputRowParser).withIgnoreInvalidRows(ignoreInvalidRows);
    }

    Set<String> exclusions = Sets.newHashSet();
    for (AggregatorFactory aggregator : aggregators) {
      exclusions.addAll(aggregator.requiredFields());
      exclusions.add(aggregator.getName());
    }

    DimensionsSpec dimensionsSpec = inputRowParser.getDimensionsSpec();
    if (dimensionsSpec == null) {
      dimensionsSpec = new DimensionsSpec(null, null, null);
    }
    TimestampSpec timestampSpec = inputRowParser.getTimestampSpec();

    // exclude timestamp from dimensions by default, unless explicitly included in the list of dimensions
    List<String> dimensionNames = dimensionsSpec.getDimensionNames();
    if (timestampSpec != null) {
      for (String timestampColumn : timestampSpec.getRequiredColumns()) {
        if (!(dimensionsSpec.hasCustomDimensions() && dimensionNames.contains(timestampColumn))) {
          exclusions.add(timestampColumn);
        }
      }
    }
    Set<String> metSet = Sets.newHashSet();
    for (AggregatorFactory aggregator : aggregators) {
      metSet.add(aggregator.getName());
    }
    Set<String> dimSet = Sets.newHashSet(dimensionNames);
    Set<String> overlap = Sets.intersection(metSet, dimSet);
    if (!overlap.isEmpty()) {
      throw new IAE(
          "Cannot have overlapping dimensions and metrics of the same name. Please change the name of the metric. Overlap: %s",
          overlap
      );
    }
    exclusions = Sets.difference(exclusions, dimSet);

    if (!exclusions.isEmpty()) {
      inputRowParser = inputRowParser.withDimensionExclusions(exclusions);
    }
    return inputRowParser;
  }

  @JsonProperty("metricsSpec")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  @JsonProperty("evaluations")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Evaluation> getEvaluations()
  {
    return evaluations;
  }

  @JsonProperty("validations")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Validation> getValidations()
  {
    return validations;
  }

  @JsonProperty
  public boolean isEnforceType()
  {
    return enforceType;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public boolean isDimensionFixed()
  {
    return dimensionFixed;
  }

  public DataSchema withDataSource(String dataSource)
  {
    return new DataSchema(dataSource,
                          parser,
                          aggregators,
                          enforceType,
                          granularitySpec,
                          evaluations,
                          validations,
                          dimensionFixed);
  }

  public DataSchema withGranularitySpec(GranularitySpec granularitySpec)
  {
    return new DataSchema(dataSource,
                          parser,
                          aggregators,
                          enforceType,
                          granularitySpec,
                          evaluations,
                          validations,
                          dimensionFixed);
  }

  public DataSchema withParser(Map<String, Object> parser)
  {
    return new DataSchema(dataSource,
                          parser,
                          aggregators,
                          enforceType,
                          granularitySpec,
                          evaluations,
                          validations,
                          dimensionFixed);
  }

  public DataSchema withValidations(List<Validation> validations)
  {
    return new DataSchema(dataSource,
                          parser,
                          aggregators,
                          enforceType,
                          granularitySpec,
                          evaluations,
                          validations,
                          dimensionFixed);
  }

  @Override
  public String toString()
  {
    return "DataSchema{" +
           "dataSource='" + dataSource + '\'' +
           ", parser=" + parser +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", enforceType=" + enforceType +
           ", granularitySpec=" + granularitySpec +
           ", evaluations=" + evaluations +
           ", validations=" + validations +
           ", dimensionFixed=" + dimensionFixed +
           '}';
  }

  // for projection pushdown
  public Set<String> getRequiredColumnNames(InputRowParser parser)
  {
    List<DimensionSchema> dimensionSchema = parser.getDimensionsSpec().getDimensions();
    if (dimensionSchema.isEmpty()) {
      return null;
    }
    Set<String> required = Sets.newHashSet();

    required.addAll(parser.getTimestampSpec().getRequiredColumns());

    for (DimensionSchema dimension : dimensionSchema) {
      required.add(dimension.getName());
    }
    for (AggregatorFactory agg : getAggregators()) {
      required.addAll(agg.requiredFields());
    }
    for (Evaluation evaluation : getEvaluations()) {
      for (String expression : evaluation.getExpressions()) {
        required.addAll(Parser.findRequiredBindings(expression));
      }
    }
    for (Validation validation : getValidations()) {
      for (String expression : validation.getExclusions()) {
        required.addAll(Parser.findRequiredBindings(expression));
      }
    }
    return required;
  }
}
