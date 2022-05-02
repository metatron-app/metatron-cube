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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.GeometryDeserializer;
import io.druid.query.RowSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.jts.JtsGeoJSONWriter;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("geojson")
public class GeoJsonFormatter implements Formatter
{
  private static final Logger log = new Logger(GeoJsonFormatter.class);

  private final String typeString;
  private final ObjectMapper mapper;
  private final List<String> inputColumns;
  private final List<String> outputColumns;

  private final int geomIndex;
  private final JtsGeoJSONWriter geoJson = new JtsGeoJSONWriter(JtsSpatialContext.GEO, null);
  private final Writer writer;
  private final Path path;
  private final FileSystem fs;

  private int counter;
  private boolean header;

  @JsonCreator
  public GeoJsonFormatter(
      @JsonProperty("outputPath") String outputPath,
      @JsonProperty("inputColumns") String[] inputColumns,
      @JsonProperty("typeString") String typeString,
      @JsonProperty("geomColumn") String geomColumn,
      @JacksonInject ObjectMapper mapper
  ) throws IOException
  {
    log.info("Applying schema : %s", typeString);
    this.path = new Path(outputPath);
    this.fs = path.getFileSystem(new Configuration());
    this.writer = new OutputStreamWriter(fs.create(path), StringUtils.UTF8_STRING);
    RowSignature signature = RowSignature.fromTypeString(typeString);
    this.inputColumns = inputColumns == null ? signature.getColumnNames() : Arrays.asList(inputColumns);
    this.outputColumns = signature.getColumnNames();
    this.geomIndex = findGeomIndex(signature, geomColumn);
    Preconditions.checkArgument(geomIndex >= 0, "cannot find geometry in %s", typeString);
    this.mapper = mapper;
    this.typeString = typeString;
  }

  private int findGeomIndex(RowSignature signature, String geomColumn)
  {
    List<String> columnNames = signature.getColumnNames();
    if (geomColumn != null) {
      return columnNames.indexOf(geomColumn);
    }
    List<ValueDesc> columnTypes = signature.getColumnTypes();
    for (int i = 0; i < signature.size(); i++) {
      if (ValueDesc.isGeometry(columnTypes.get(i))) {
        return i;
      }
    }
    return -1;
  }

  @Override
  public void write(Map<String, Object> datum) throws IOException
  {
    if (!header) {
      writer.write("{ \"type\": \"FeatureCollection\", \"features\": [\n");
      header = true;
    } else {
      writer.write(",\n");
    }
    writer.write("  { \"type\": \"Feature\", ");
    if (geomIndex >= 0) {
      writer.write("\"geometry\": ");
      Object value = datum.get(inputColumns.get(geomIndex));
      geoJson.write(writer, toGeometry(value));
    }
    if (inputColumns.size() > 1) {
      writer.write(", \"properties\": {");
      for (int i = 0; i < inputColumns.size(); i++) {
        if (i == geomIndex) {
          continue;
        }
        if (geomIndex != 0 && i > 0 || geomIndex == 0 && i == 1) {
          writer.write(",");
        }
        Object object = datum.get(inputColumns.get(i));
        writer.write("\"");
        writer.write(outputColumns.get(i));
        writer.write("\" :");
        writer.write(mapper.writeValueAsString(object));
      }
      writer.write("} ");
    }
    writer.write("}");
    counter++;
  }

  private Geometry toGeometry(Object value) throws IOException
  {
    if (value == null || value instanceof Geometry) {
      return (Geometry) value;
    } else if (value instanceof byte[]) {
      return GeometryDeserializer.deserialize((byte[]) value);
    }
    throw new IAE("cannot convert %s to geometry", value.getClass());
  }

  @Override
  public Map<String, Object> close() throws IOException
  {
    writer.write("\n]}");
    writer.close();
    long length = fs.getFileStatus(path).getLen();
    return ImmutableMap.<String, Object>of(
        "rowCount", counter,
        "typeString", typeString,
        "data", ImmutableMap.of(path.toString(), length)
    );
  }
}
