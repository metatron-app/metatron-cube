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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.common.base.Preconditions;
import io.druid.common.guava.Sequence;
import io.druid.java.util.common.IAE;
import io.druid.query.GeometryDeserializer;
import io.druid.query.PostProcessingOperators;
import io.druid.query.Query;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.io.LegacyShapeWriter;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;

@JsonTypeName("geojson")
public class GeoJsonDecorator implements OutputDecorator<Object[]>
{
  private final NumberFormat nf = LegacyShapeWriter.makeNumberFormat(6);

  private final int geomIndex;
  private final List<String> columnNames;

  @JsonCreator
  public GeoJsonDecorator(
      @JsonProperty("geomIndex") Integer geomIndex,
      @JsonProperty("columnNames") List<String> columnNames
  )
  {
    this.geomIndex = Preconditions.checkNotNull(geomIndex, "'geomIndex' cannot be null");
    this.columnNames = columnNames;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<Object[]> prepare(Query query, Sequence sequence)
  {
    if (PostProcessingOperators.returns(query) == Object[].class) {
      return sequence;
    }
    if (query instanceof Query.ArrayOutputSupport) {
      return ((Query.ArrayOutputSupport) query).array(sequence);
    }
    return sequence;  // should not throw exception here
  }

  @Override
  public void start(JsonGenerator jgen, SerializerProvider provider) throws IOException
  {
    jgen.writeStartObject();
    jgen.writeObjectField("type", "FeatureCollection");
    jgen.writeFieldName("features");
    jgen.writeStartArray();
  }

  @Override
  public void serialize(JsonGenerator jgen, SerializerProvider provider, Object[] object) throws IOException
  {
    jgen.writeStartObject();
    jgen.writeObjectField("type", "Feature");
    if (geomIndex >= 0 && object[geomIndex] != null) {
      jgen.writeFieldName("geometry");
      write(jgen, nf, toGeometry(object[geomIndex]));
    }
    if (columnNames.size() > 1) {
      jgen.writeFieldName("properties");
      jgen.writeStartObject();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i != geomIndex) {
          jgen.writeObjectField(columnNames.get(i), object[i]);
        }
      }
      jgen.writeEndObject();
    }
    jgen.writeEndObject();
  }

  @Override
  public void end(JsonGenerator jgen, SerializerProvider provider) throws IOException
  {
    jgen.writeEndArray();
    jgen.writeEndObject();
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

  // copied from JtsGeoJSONWriter and rewritten for JsonGenerator
  private static void write(JsonGenerator jgen, NumberFormat nf, Geometry geom) throws IOException
  {
    if (geom instanceof Point) {
      Point v = (Point) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "Point");
      jgen.writeFieldName("coordinates");
      write(jgen, nf, v.getCoordinate());
      jgen.writeEndObject();
    } else if (geom instanceof Polygon) {
      jgen.writeStartObject();
      jgen.writeObjectField("type", "Polygon");
      jgen.writeFieldName("coordinates");
      write(jgen, nf, (Polygon) geom);
      jgen.writeEndObject();
    } else if (geom instanceof LineString) {
      LineString v = (LineString) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "LineString");
      jgen.writeFieldName("coordinates");
      write(jgen, nf, v.getCoordinates());
      jgen.writeEndObject();
    } else if (geom instanceof MultiPoint) {
      MultiPoint v = (MultiPoint) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "MultiPoint");
      jgen.writeFieldName("coordinates");
      write(jgen, nf, v.getCoordinates());
      jgen.writeEndObject();
    } else if (geom instanceof MultiLineString) {
      MultiLineString v = (MultiLineString) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "MultiLineString");
      jgen.writeFieldName("coordinates");
      jgen.writeStartArray();
      for (int i = 0; i < v.getNumGeometries(); i++) {
        write(jgen, nf, v.getGeometryN(i).getCoordinates());
      }
      jgen.writeEndArray();
      jgen.writeEndObject();
    } else if (geom instanceof MultiPolygon) {
      MultiPolygon v = (MultiPolygon) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "MultiPolygon");
      jgen.writeFieldName("coordinates");
      jgen.writeStartArray();
      for (int i = 0; i < v.getNumGeometries(); i++) {
        write(jgen, nf, (Polygon) v.getGeometryN(i));
      }
      jgen.writeEndArray();
      jgen.writeEndObject();
    } else if (geom instanceof GeometryCollection) {
      GeometryCollection v = (GeometryCollection) geom;
      jgen.writeStartObject();
      jgen.writeObjectField("type", "GeometryCollection");
      jgen.writeFieldName("geometries");
      jgen.writeStartArray();
      for (int i = 0; i < v.getNumGeometries(); i++) {
        write(jgen, nf, v.getGeometryN(i));
      }
      jgen.writeEndArray();
      jgen.writeEndObject();
    } else {
      throw new UnsupportedOperationException("unknown: " + geom);
    }
  }

  private static void write(JsonGenerator jgen, NumberFormat nf, Coordinate coord) throws IOException
  {
    jgen.writeStartArray();
    jgen.writeNumber(nf.format(coord.x));
    jgen.writeNumber(nf.format(coord.y));
    jgen.writeEndArray();
  }

  private static void write(JsonGenerator jgen, NumberFormat nf, Coordinate[] coords) throws IOException
  {
    jgen.writeStartArray();
    for (Coordinate coord : coords) {
      write(jgen, nf, coord);
    }
    jgen.writeEndArray();
  }

  private static void write(JsonGenerator jgen, NumberFormat nf, Polygon p) throws IOException
  {
    jgen.writeStartArray();
    write(jgen, nf, p.getExteriorRing().getCoordinates());
    for (int i = 0; i < p.getNumInteriorRing(); i++) {
      write(jgen, nf, p.getInteriorRingN(i).getCoordinates());
    }
    jgen.writeEndArray();
  }
}
