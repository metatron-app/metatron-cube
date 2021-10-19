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

package io.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import io.druid.data.input.ExpressionTimestampSpec;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.Granularity;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TypeStringResolver extends AbstractResolver
{
  private final String typeString;

  @JsonCreator
  public TypeStringResolver(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") String paths,
      @JsonProperty("recursive") boolean recursive,
      @JsonProperty("typeString") String typeString,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    super(basePath, paths, recursive, timeExpression, segmentGranularity);
    this.typeString = Preconditions.checkNotNull(typeString, "'typeString' should not be null");
  }

  @Override
  public FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    List<String> resolved = resolve(basePath, paths, recursive);
    if (resolved.size() == 0) {
      throw new IAE("Cannot resolve path %s + %s", basePath, paths);
    }
    String extension = Objects.toString(properties.get("extension"), null);
    String inputFormat = Objects.toString(properties.get("inputFormat"), null);

    RowSignature signature = RowSignature.fromTypeString(typeString, ValueDesc.STRING);
    TimestampSpec timestampSpec = null;
    if (timeExpression != null) {
      timestampSpec = new ExpressionTimestampSpec(timeExpression);
    }
    ObjectMapper mapper = walker.getMapper();
    List<String> dimensions = signature.extractDimensionCandidates();
    List<AggregatorFactory> metrics = rewriteMetrics(
        signature.extractMetricCandidates(Sets.newHashSet(dimensions)), properties, mapper
    );
    GranularitySpec granularity = UniformGranularitySpec.of(segmentGranularity);
    Map<String, Object> parser = Maps.newHashMap(properties);
    if (!parser.containsKey("type")) {
      parser.put("type", "csv.stream");
    }
    parser.put("columns", signature.getColumnNames());
    parser.put("timestampSpec", mapper.convertValue(timestampSpec, ObjectMappers.MAP_REF));
    parser.put("dimensionsSpec", DimensionsSpec.ofStringDimensions(dimensions));
    DataSchema dataSchema = new DataSchema(dataSource, parser, metrics.toArray(new AggregatorFactory[0]), granularity);
    BaseTuningConfig config = tuningConfigFromProperties(properties, mapper);
    return new FileLoadSpec(null, resolved, extension, inputFormat, dataSchema, null, null, config, properties);
  }
}
