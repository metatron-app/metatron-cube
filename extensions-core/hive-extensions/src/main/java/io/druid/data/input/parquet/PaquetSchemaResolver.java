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

package io.druid.data.input.parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.data.input.ExpressionTimestampSpec;
import io.druid.data.input.IncrementTimestampSpec;
import io.druid.data.input.RelayTimestampSpec;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.granularity.Granularity;
import io.druid.indexer.path.PathUtil;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.server.FileLoadSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("parquet")
public class PaquetSchemaResolver implements FileLoadSpec.Resolver
{
  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)
  private final List<String> paths;
  private final boolean recursive;

  private final String timeExpression;
  private final Granularity segmentGranularity;

  @JsonCreator
  public PaquetSchemaResolver(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("paths") String paths,
      @JsonProperty("recursive") boolean recursive,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    Preconditions.checkArgument(basePath != null || !StringUtils.isNullOrEmpty(paths), "No path");
    this.basePath = basePath;
    this.paths = paths == null ? null : Arrays.asList(paths.split(","));
    this.recursive = recursive;
    this.timeExpression = timeExpression;
    this.segmentGranularity = segmentGranularity;
  }

  @Override
  public FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    Path base = basePath == null ? null : new Path(basePath);
    List<String> resolved = PathUtil.resolve(base, paths, recursive);
    if (resolved.size() == 0) {
      throw new IAE("Cannot resolve path %s + %s", base, paths);
    }
    // from first file
    Path path = base == null ? new Path(resolved.get(0)) : new Path(base, resolved.get(0));
    ParquetMetadata metadata = ParquetFileReader.readFooter(
        new Configuration(), path, ParquetMetadataConverter.NO_FILTER
    );

    TimestampSpec timestampSpec = null;
    if (timeExpression != null) {
      timestampSpec = new ExpressionTimestampSpec(timeExpression);
    }
    List<String> dimensions = Lists.newArrayList();
    List<AggregatorFactory> agggregators = Lists.newArrayList();

    MessageType messageType = metadata.getFileMetaData().getSchema();
    for (Type field : messageType.getFields()) {
      final OriginalType originalType = field.getOriginalType();
      if (originalType != null) {
        switch (originalType) {
          case UTF8:
            dimensions.add(field.getName());
            continue;
          case DATE:
            if (timestampSpec == null) {
              timestampSpec = new RelayTimestampSpec(field.getName());
              continue;
            }
          case DECIMAL:
            agggregators.add(RelayAggregatorFactory.of(field.getName(), ValueDesc.DECIMAL));
            continue;
        }
      }
      if (field.isPrimitive()) {
        final PrimitiveType primitive = field.asPrimitiveType();
        switch (primitive.getPrimitiveTypeName()) {
          case INT32:
          case INT64:
            agggregators.add(RelayAggregatorFactory.of(field.getName(), ValueDesc.LONG));
            continue;
          case BOOLEAN:
            agggregators.add(RelayAggregatorFactory.of(field.getName(), ValueDesc.BOOLEAN));
            continue;
          case FLOAT:
            agggregators.add(RelayAggregatorFactory.of(field.getName(), ValueDesc.FLOAT));
            continue;
          case DOUBLE:
            agggregators.add(RelayAggregatorFactory.of(field.getName(), ValueDesc.DOUBLE));
            continue;
        }
      }
      throw new UnsupportedOperationException("Unknown type " + field);
    }
    if (timestampSpec == null) {
      timestampSpec = IncrementTimestampSpec.dummy();
    }
    InputRowParser parser = new MapInputRowParser(
        new TimeAndDimsParseSpec(timestampSpec, DimensionsSpec.ofStringDimensions(dimensions))
    );
    Map<String, Object> spec = walker.getObjectMapper().convertValue(parser, ObjectMappers.MAP_REF);
    GranularitySpec granularity = UniformGranularitySpec.of(segmentGranularity);
    DataSchema schema = new DataSchema(
        dataSource, spec, agggregators.toArray(new AggregatorFactory[0]), false, granularity, null, null, true
    );
    return new FileLoadSpec(
        basePath,
        resolved,
        "druid-hive-extensions",
        HiveParquetInputFormat.class.getName(),
        schema,
        null,
        null,
        null,
        null
    );
  }
}
