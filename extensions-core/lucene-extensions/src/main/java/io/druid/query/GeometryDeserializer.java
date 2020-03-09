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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.druid.data.input.BytesInputStream;
import io.druid.java.util.common.IAE;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LinearRing;

import java.io.IOException;

public class GeometryDeserializer extends JsonDeserializer<Geometry>
{
  @Override
  public Geometry deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
  {
    return deserialize(jp.getBinaryValue());
  }

  public static Geometry deserialize(byte[] bytes) throws IOException
  {
    final BytesInputStream input = new BytesInputStream(bytes);
    final byte code = input.readByte();
    if (code == 0x7f) {
      final Geometry[] geometries = new Geometry[input.readUnsignedVarInt()];
      for (int i = 0; i < geometries.length; i++) {
        geometries[i] = _deserialize(input.readByte(), input);
      }
      return new GeometryCollection(geometries, GeomUtils.GEOM_FACTORY);
    } else {
      return _deserialize(code, input);
    }
  }

  private static Geometry _deserialize(final int code, final BytesInputStream input) throws IOException
  {
    if (code == 0x01) {
      final Coordinate coord = new Coordinate(input.readDouble(), input.readDouble(), Double.NaN);
      return GeomUtils.GEOM_FACTORY.createPoint(coord);
    } else if (code == 0x02) {
      final Coordinate[] coordinates = new Coordinate[input.readUnsignedVarInt()];
      for (int i = 0; i < coordinates.length; i++) {
        coordinates[i] = new Coordinate(input.readDouble(), input.readDouble(), Double.NaN);
      }
      return GeomUtils.GEOM_FACTORY.createLineString(coordinates);
    } else if (code == 0x03) {
      final Coordinate[] shell = new Coordinate[input.readUnsignedVarInt()];
      for (int i = 0; i < shell.length; i++) {
        shell[i] = new Coordinate(input.readDouble(), input.readDouble(), Double.NaN);
      }
      final LinearRing[] holes = new LinearRing[input.readUnsignedVarInt()];
      for (int i = 0; i < holes.length; i++) {
        Coordinate[] hole = new Coordinate[input.readUnsignedVarInt()];
        for (int j = 0; j < hole.length; j++) {
          hole[j] = new Coordinate(input.readDouble(), input.readDouble(), Double.NaN);
        }
        holes[i] = GeomUtils.GEOM_FACTORY.createLinearRing(hole);
      }
      return GeomUtils.GEOM_FACTORY.createPolygon(GeomUtils.GEOM_FACTORY.createLinearRing(shell), holes);
    } else {
      throw new IAE("?? %d", code);
    }
  }
}
