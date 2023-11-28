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

package io.druid.query.segment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.BytesInputStream;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StreamUtils;
import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.timeline.DataSegment;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;

public class SegmentLocation
{
  public static final TypeReference<SegmentLocation> TYPE_REFERENCE = new TypeReference<SegmentLocation>()
  {
  };

  public static final JsonDeserializer<SegmentLocation> DESERIALIZER = new JsonDeserializer<SegmentLocation>()
  {
    @Override
    public SegmentLocation deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
    {
      JsonToken token = jp.getCurrentToken();
      Preconditions.checkArgument(token == JsonToken.START_OBJECT);

      File location = GuavaUtils.createTemporaryDirectory("temp", ".segment");
      File version = FileSmoosher.versionFile(location);
      File meta = FileSmoosher.metaFile(location);
      File chunk = FileSmoosher.chunkFile(location, 0);

      for (token = jp.nextToken(); token == JsonToken.FIELD_NAME; token = jp.nextToken()) {
        String fieldName = jp.getText();
        Preconditions.checkArgument(jp.nextToken() == JsonToken.VALUE_EMBEDDED_OBJECT);

        File target;
        switch (fieldName) {
          case "meta":
            target = FileSmoosher.metaFile(location);
            break;
          case "chunk":
            target = FileSmoosher.chunkFile(location, 0);
            break;
          case "version":
            target = FileSmoosher.versionFile(location);
            break;
          default:
            throw new ISE("invalid field name [%s]", fieldName);
        }
        StreamUtils.copyAndClose(
            new BytesInputStream(jp.getBinaryValue()),
            new BufferedOutputStream(new FileOutputStream(target))
        );
      }
      Preconditions.checkArgument(token == JsonToken.END_OBJECT);
      return new SegmentLocation(null, location);
    }
  };

  private final DataSegment segment;
  private final File location;

  public SegmentLocation(DataSegment segment, File location)
  {
    this.segment = segment;
    this.location = location;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty
  public File getLocation()
  {
    return location;
  }

  public SegmentLocation withSegment(DataSegment segment)
  {
    return new SegmentLocation(segment, location);
  }

  @JsonValue
  public Map<String, Object> output()
  {
    File version = FileSmoosher.versionFile(location);
    File meta = FileSmoosher.metaFile(location);
    File chunk = FileSmoosher.chunkFile(location, 0);
    try {
      return ImmutableMap.of(
          "version", new FileInputStream(version),
          "meta", new FileInputStream(meta),
          "chunk", new FileInputStream(chunk)
      );
    }
    catch (FileNotFoundException e) {
      throw new ISE(e, "invalid location [%s] for segment [%s]", location, segment);
    }
  }
}
