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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.data.input.Evaluation;
import io.druid.data.input.InputRowParsers;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.Validation;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.IncrementalIndexSchema;

import java.util.List;
import java.util.Map;

public class TestLoadSpec extends IncrementalIndexSchema
{
  private final Map<String, Object> parser;
  private final List<String> columns;
  private final TimestampSpec timestampSpec;

  private final boolean enforceType;
  private final List<Evaluation> evaluations;
  private final List<Validation> validations;

  private final IndexSpec indexSpec;

  @JsonCreator
  public TestLoadSpec(
      @JsonProperty("minTimestamp") long minTimestamp,
      @JsonProperty("queryGran") Granularity queryGran,
      @JsonProperty("segmentGran") Granularity segmentGran,
      @JsonProperty("parser") Map<String, Object> parser,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] metricsSpec,
      @JsonProperty("evaluations") List<Evaluation> evaluations,
      @JsonProperty("validations") List<Validation> validations,
      @JsonProperty("enforceType") boolean enforceType,
      @JsonProperty("rollup") boolean rollup,
      @JsonProperty("fixedSchema") boolean fixedSchema,
      @JsonProperty("indexSpec") IndexSpec indexSpec
  )
  {
    super(
        minTimestamp,
        queryGran,
        segmentGran,
        dimensionsSpec,
        metricsSpec,
        rollup,
        fixedSchema,
        false,
        indexSpec == null ? null : indexSpec.getColumnDescriptors()
    );
    this.parser = parser == null ? Maps.<String, Object>newHashMap() : parser;
    this.columns = columns;
    this.timestampSpec = timestampSpec;
    this.evaluations = evaluations == null ? ImmutableList.<Evaluation>of() : evaluations;
    this.validations = validations == null ? ImmutableList.<Validation>of() : validations;
    this.enforceType = enforceType;
    this.indexSpec = indexSpec;
  }

  public InputRowParser getParser(ObjectMapper mapper, boolean ignoreInvalidRows)
  {
    Map<String, Object> parsing = Maps.newHashMap(parser);
    parsing.put("dimensionsSpec", getDimensionsSpec());
    parsing.put("timestampSpec", timestampSpec);
    parsing.put("columns", columns);

    InputRowParser rowParser = parser.containsKey("type") ? mapper.convertValue(parsing, InputRowParser.class) : null;
    if (rowParser == null) {
      rowParser = new StringInputRowParser(mapper.convertValue(parsing, ParseSpec.class), null);
    }
    return InputRowParsers.wrap(
        rowParser,
        getMetrics(),
        evaluations,
        validations,
        enforceType,
        ignoreInvalidRows
    );
  }

  public IndexSpec getIndexingSpec()
  {
    return indexSpec == null ? new IndexSpec() : indexSpec;
  }
}
