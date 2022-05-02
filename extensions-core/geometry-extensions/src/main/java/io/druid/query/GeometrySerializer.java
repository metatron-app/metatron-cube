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

package io.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.IAE;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.io.IOException;

public class GeometrySerializer extends JsonSerializer<Geometry>
{
  @Override
  public void serialize(Geometry value, JsonGenerator jgen, SerializerProvider provider) throws IOException
  {
    BytesOutputStream output = new BytesOutputStream();
    if (value instanceof GeometryCollection) {
      output.writeByte(0x7f);
      output.writeUnsignedVarInt(value.getNumGeometries());
      for (int i = 0; i < value.getNumGeometries(); i++) {
        _serialize(value.getGeometryN(i), output);
      }
    } else {
      _serialize(value, output);
    }
    jgen.writeBinary(output.toByteArray());
  }

  private void _serialize(Geometry value, BytesOutputStream output) throws IOException
  {
    if (value instanceof Point) {
      Coordinate coord = value.getCoordinate();
      output.writeByte(0x01);
      output.writeDouble(coord.x);
      output.writeDouble(coord.y);
    } else if (value instanceof LineString) {
      Coordinate[] coordinates = value.getCoordinates();
      output.writeByte(0x02);
      output.writeUnsignedVarInt(coordinates.length);
      for (Coordinate coord : coordinates) {
        output.writeDouble(coord.x);
        output.writeDouble(coord.y);
      }
    } else if (value instanceof Polygon) {
      Polygon polygon = (Polygon) value;
      output.writeByte(0x03);
      LineString shell = polygon.getExteriorRing();
      output.writeUnsignedVarInt(shell.getNumPoints());
      for (Coordinate coord : shell.getCoordinates()) {
        output.writeDouble(coord.x);
        output.writeDouble(coord.y);
      }
      output.writeUnsignedVarInt(polygon.getNumInteriorRing());
      for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
        LineString hole = polygon.getInteriorRingN(i);
        output.writeUnsignedVarInt(hole.getNumPoints());
        for (Coordinate coord : hole.getCoordinates()) {
          output.writeDouble(coord.x);
          output.writeDouble(coord.y);
        }
      }
    } else {
      throw new IAE("?? %s", value.getClass());
    }
  }
}
