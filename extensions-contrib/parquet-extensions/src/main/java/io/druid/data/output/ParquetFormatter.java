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

package io.druid.data.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * to validate, use hive --orcfiledump -d path
 */
@JsonTypeName("parquet")
public class ParquetFormatter implements Formatter
{
  private static final Logger log = new Logger(ParquetFormatter.class);

  private final String typeString;
  private final List<String> inputColumns;
  private final Path path;
  private final FileSystem fs;
  private final Schema schema;
  private final ParquetWriter<GenericData.Record> writer;

  private int counter;

  @JsonCreator
  public ParquetFormatter(
      @JsonProperty("outputPath") String outputPath,
      @JsonProperty("inputColumns") String[] inputColumns,
      @JsonProperty("typeString") String typeString
  ) throws IOException
  {
    log.info("Applying schema : %s", typeString);
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(ParquetFormatter.class.getClassLoader());
    try {
      final Configuration conf = new Configuration();
      StringBuilder builder = new StringBuilder(
          "{\"namespace\": \"metatron.app\",\"type\": \"record\",\"name\": \"metatron\",\"fields\": ["
      );
      int header = builder.length();
      for (String element : typeString.split(",")) {
        if (builder.length() > header) {
          builder.append(',');
        }
        final int index = element.indexOf(':');
        builder.append("{\"name\": \"").append(element, 0, index).append("\", ");
        builder.append("\"type\": \"").append(element.substring(index + 1)).append("\"}");
      }
      builder.append("]}");
      Schema.Parser parser = new Schema.Parser().setValidate(true);
      this.schema = parser.parse(builder.toString());

      this.path = new Path(outputPath);
      this.inputColumns = inputColumns == null ? getFieldNames(schema.getFields()) : Arrays.asList(inputColumns);
      this.writer = AvroParquetWriter.<GenericData.Record>builder(path)
          .withSchema(schema)
          .withCompressionCodec(CompressionCodecName.GZIP)
          .withConf(conf)
          .withPageSize(4 * 1024 * 1024) //For compression
          .withRowGroupSize(16 * 1024 * 1024) //For write buffering (Page size)
          .build();
      this.fs = path.getFileSystem(conf);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
    this.typeString = typeString;
  }

  private List<String> getFieldNames(List<Schema.Field> fields)
  {
    List<String> names = Lists.newArrayList();
    for (Schema.Field field : fields) {
      names.add(field.name());
    }
    return names;
  }

  @Override
  public void write(Map<String, Object> datum) throws IOException
  {
    final GenericData.Record record = new GenericData.Record(schema);
    for (int i = 0; i < inputColumns.size(); i++) {
      record.put(i, datum.get(inputColumns.get(i)));
    }
    writer.write(record);
    counter++;
  }

  @Override
  public Map<String, Object> close() throws IOException
  {
    writer.close();
    long length = fs.getFileStatus(path).getLen();
    return ImmutableMap.<String, Object>of(
        "rowCount", counter,
        "typeString", typeString,
        "data", ImmutableMap.of(path.toString(), length)
    );
  }
}
