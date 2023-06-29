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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.Granularity;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.StringUtils;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.RowSignature;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.RelayAggregatorFactory;
import io.druid.segment.incremental.BaseTuningConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.server.AbstractResolver;
import io.druid.server.FileLoadSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.data.shapefile.dbf.DbaseFileHeader;
import org.geotools.data.shapefile.dbf.DbaseFileReader;
import org.geotools.data.shapefile.files.FileReader;
import org.geotools.data.shapefile.files.ShpFileType;
import org.geotools.data.shapefile.files.ShpFiles;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("shape")
public class ShapeFileResolver extends AbstractResolver
{
  private final String encoding;

  @JsonCreator
  public ShapeFileResolver(
      @JsonProperty("basePath") String basePath,
      @JsonProperty("encoding") String  encoding,
      @JsonProperty("recursive") boolean recursive,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity
  )
  {
    super(basePath, null, false, timeExpression, segmentGranularity);
    this.encoding = encoding;
  }

  @Override
  public FileLoadSpec resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    ClassLoader loader = ShapeFileResolver.class.getClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      return _resolve(dataSource, walker);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  public FileLoadSpec _resolve(String dataSource, QuerySegmentWalker walker) throws IOException
  {
    Path base = new Path(basePath);
    Configuration configuration = new Configuration();
    FileSystem fs = base.getFileSystem(configuration);
    FileStatus attributeFile = Preconditions.checkNotNull(
        ShapeFileInputFormat.findFile(fs.listStatus(base), ".dbf"), "Failed to find .dbf"
    );
    final Charset charset = Charset.forName(Optional.fromNullable(encoding).or(StringUtils.UTF8_STRING));
    final DbaseFileReader attrReader = new DbaseFileReader(new ShpFiles(File.createTempFile("druid_shape", ".dbf"))
    {
      @Override
      public ReadableByteChannel getReadChannel(ShpFileType type, FileReader requestor) throws IOException
      {
        if (type == ShpFileType.DBF) {
          return Channels.newChannel(fs.open(attributeFile.getPath()));
        } else {
          throw new IllegalArgumentException("Not supported type " + type);
        }
      }
    }, false, charset);

    final DbaseFileHeader header = attrReader.getHeader();
    final List<String> fieldNames = Lists.newArrayList();
    final List<ValueDesc> fieldTypes = Lists.newArrayList();
    for (int i = 0; i < header.getNumFields(); i++) {
      fieldNames.add(header.getFieldName(i));
      Class clazz = header.getFieldClass(i);
      if (clazz == Integer.class || clazz == Long.class) {
        fieldTypes.add(ValueDesc.LONG);
      } else if (clazz == Double.class) {
        fieldTypes.add(ValueDesc.DOUBLE);
      } else if (clazz == Boolean.class) {
        fieldTypes.add(ValueDesc.BOOLEAN);
      } else if (clazz == Date.class || clazz == Timestamp.class) {
        fieldTypes.add(ValueDesc.DATETIME);
      } else {
        fieldTypes.add(ValueDesc.STRING);
      }
    }
    String extension = Objects.toString(properties.get("extension"), "druid-geotools-extensions");
    String inputFormat = Objects.toString(properties.get("inputFormat"), "io.druid.data.input.ShapeFileInputFormat");

    RowSignature signature = RowSignature.of(fieldNames, fieldTypes);
    TimestampSpec timestampSpec = null;
    if (timeExpression != null) {
      timestampSpec = new ExpressionTimestampSpec(timeExpression);
    }
    ObjectMapper mapper = walker.getMapper();
    List<String> dimensions = signature.extractDimensionCandidates();
    List<AggregatorFactory> relays = signature.exclude(Sets.newHashSet(dimensions))
                                              .stream()
                                              .map(p -> RelayAggregatorFactory.of(p.lhs, p.rhs))
                                              .collect(Collectors.toList());
    List<AggregatorFactory> metrics = rewriteMetrics(relays, properties, mapper);
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
    return new FileLoadSpec(null, Arrays.asList(basePath), extension, inputFormat, dataSchema, null, null, config, properties);
  }
}
