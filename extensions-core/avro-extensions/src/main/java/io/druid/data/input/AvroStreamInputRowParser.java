/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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
import io.druid.data.ParsingFail;
import io.druid.data.input.avro.AvroBytesDecoder;
import io.druid.data.input.avro.GenericRecordAsMap;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public class AvroStreamInputRowParser implements InputRowParser<ByteBuffer>
{
  private final ParseSpec parseSpec;
  private final List<String> dimensions;
  private final AvroBytesDecoder avroBytesDecoder;

  @JsonCreator
  public AvroStreamInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("avroBytesDecoder") AvroBytesDecoder avroBytesDecoder
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.avroBytesDecoder = avroBytesDecoder;
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    return parseGenericRecord(avroBytesDecoder.parse(input), parseSpec, dimensions, false);
  }

  protected static InputRow parseGenericRecord(
      GenericRecord record, ParseSpec parseSpec, List<String> dimensions, boolean fromPigAvroStorage
  )
  {
    try {
      GenericRecordAsMap genericRecordAsMap = new GenericRecordAsMap(record, fromPigAvroStorage);
      TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
      DateTime dateTime = timestampSpec.extractTimestamp(genericRecordAsMap);
      return new MapBasedInputRow(dateTime, dimensions, genericRecordAsMap);
    }
    catch (Exception e) {
      throw ParsingFail.propagate(record, e);
    }
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return parseSpec.getTimestampSpec();
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return parseSpec.getDimensionsSpec();
  }

  @JsonProperty
  public AvroBytesDecoder getAvroBytesDecoder()
  {
    return avroBytesDecoder;
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new AvroStreamInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions)),
        avroBytesDecoder
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AvroStreamInputRowParser that = (AvroStreamInputRowParser) o;

    if (!parseSpec.equals(that.parseSpec)) {
      return false;
    }
    if (!dimensions.equals(that.dimensions)) {
      return false;
    }
    return avroBytesDecoder.equals(that.avroBytesDecoder);
  }

  @Override
  public int hashCode()
  {
    int result = parseSpec.hashCode();
    result = 31 * result + dimensions.hashCode();
    result = 31 * result + avroBytesDecoder.hashCode();
    return result;
  }
}
