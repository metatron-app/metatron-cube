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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
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
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.orc.OrcProto;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("orc")
public class OrcSchemaResolver implements FileLoadSpec.Resolver
{
  private final String basePath;  // optional absolute path (paths in elements are regarded as relative to this)
  private final List<String> paths;
  private final boolean recursive;

  private final String timeExpression;
  private final Granularity segmentGranularity;

  @JsonCreator
  public OrcSchemaResolver(
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

  private static final int DIMENSION_THRESHOLD = 32;

  @Override
  public FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = OrcSchemaResolver.class.getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      return _resolve(dataSource, walker);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  private FileLoadSpec _resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    Path base = basePath == null ? null : new Path(basePath);
    List<String> resolved = PathUtil.resolve(base, paths, recursive);
    if (resolved.size() == 0) {
      throw new IAE("Cannot resolve path %s + %s", base, paths);
    }
    // from first file
    Path path = base == null ? new Path(resolved.get(0)) : new Path(base, resolved.get(0));
    Reader reader = OrcFile.createReader(path, OrcFile.readerOptions(new Configuration()));

    List<OrcProto.Type> types = reader.getTypes();
    List<OrcProto.ColumnStatistics> statistics = reader.getOrcProtoFileStatistics();

    List<String> dimensions = Lists.newArrayList();
    List<AggregatorFactory> agggregators = Lists.newArrayList();
    StringBuilder typeString = new StringBuilder("struct<");

    TimestampSpec timestampSpec = null;
    if (timeExpression != null) {
      timestampSpec = new ExpressionTimestampSpec(timeExpression);
    }
    final OrcProto.Type root = types.get(0);
    final int fieldCount = root.getSubtypesCount();
    for (int i = 0; i < fieldCount; i++) {
      if (i > 0) {
        typeString.append(',');
      }
      final int index = root.getSubtypes(i);
      final OrcProto.Type type = types.get(index);
      final OrcProto.ColumnStatistics stat = statistics.get(index);
      final String fieldName = root.getFieldNames(i);
      switch (type.getKind()) {
        case FLOAT:
          typeString.append(fieldName).append(':').append(ValueDesc.FLOAT_TYPE);
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.FLOAT));
          continue;
        case DOUBLE:
          typeString.append(fieldName).append(':').append(ValueDesc.DOUBLE_TYPE);
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.DOUBLE));
          continue;
        case BOOLEAN:
          typeString.append(fieldName).append(':').append(ValueDesc.BOOLEAN_TYPE);
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.BOOLEAN));
          continue;
        case BYTE:
          typeString.append(fieldName).append(':').append("tinyint");
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.LONG));
        case SHORT:
          typeString.append(fieldName).append(':').append("smallint");
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.LONG));
          continue;
        case INT:
          typeString.append(fieldName).append(':').append("int");
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.LONG));
          continue;
        case LONG:
          typeString.append(fieldName).append(':').append("bigint");
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.LONG));
          continue;
        case CHAR:
        case STRING:
        case VARCHAR:
          typeString.append(fieldName).append(':').append(ValueDesc.STRING_TYPE);
          if (stat.getSerializedSize() / stat.getNumberOfValues() < DIMENSION_THRESHOLD) {
            dimensions.add(fieldName);
          } else {
            agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.STRING));
          }
          continue;
        case DECIMAL:
          agggregators.add(RelayAggregatorFactory.of(fieldName, ValueDesc.DECIMAL));
          continue;
        case DATE:
        case TIMESTAMP:
          if (timestampSpec == null) {
            timestampSpec = new RelayTimestampSpec(fieldName);
            continue;
          }
        case BINARY:
        case STRUCT:
        case UNION:
        case MAP:
        case LIST:
        default:
          throw new UnsupportedOperationException("Unknown type " + type.getKind());
      }
    }
    typeString.append('>');

    if (timestampSpec == null) {
      timestampSpec = IncrementTimestampSpec.dummy();
    }
    ParseSpec parseSpec = new TimeAndDimsParseSpec(timestampSpec, DimensionsSpec.ofStringDimensions(dimensions));
    InputRowParser parser = new OrcHadoopInputRowParser(parseSpec, typeString.toString(), null);
    Map<String, Object> spec = walker.getMapper().convertValue(parser, ObjectMappers.MAP_REF);
    GranularitySpec granularity = UniformGranularitySpec.of(segmentGranularity);
    DataSchema schema = new DataSchema(
        dataSource, spec, agggregators.toArray(new AggregatorFactory[0]), false, granularity, null, null, true
    );
    return new FileLoadSpec(
        basePath,
        resolved,
        "druid-orc-extensions",
        OrcNewInputFormat.class.getName(),
        schema,
        null,
        null,
        null,
        null
    );
  }
}
