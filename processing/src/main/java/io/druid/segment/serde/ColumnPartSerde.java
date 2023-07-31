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

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.segment.bitmap.BitSetInvertedIndexingSpec;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitSlicedBitmaps;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.HistogramBitmaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "complex", value = ComplexColumnPartSerde.class),
    @JsonSubTypes.Type(name = "float", value = FloatGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "double", value = DoubleGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "long", value = LongGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "string", value = StringColumnPartSerde.class),
    @JsonSubTypes.Type(name = "stringDictionary", value = DictionaryEncodedColumnPartSerde.class),
    @JsonSubTypes.Type(name = "histogram", value = HistogramBitmaps.SerDe.class),
    @JsonSubTypes.Type(name = "bsb", value = BitSlicedBitmaps.SerDe.class),
    @JsonSubTypes.Type(name = "boolean", value = BooleanColumnPartSerde.class),
    @JsonSubTypes.Type(name = "bitsetInverted", value = BitSetInvertedIndexingSpec.SerDe.class),
    @JsonSubTypes.Type(name = "struct", value = StructColumnPartSerde.class),
    @JsonSubTypes.Type(name = "map", value = MapColumnPartSerde.class),
    @JsonSubTypes.Type(name = "array", value = ArrayColumnPartSerde.class),
    @JsonSubTypes.Type(name = "enum", value = EnumColumnPartSerde.class),
    @JsonSubTypes.Type(name = "null", value = NullColumnPartSerde.class),
})
public interface ColumnPartSerde
{
  byte LZF_FIXED = 0x1;     // DO NOT USE ON GenericIndexed (this fuck conflicts with GenericIndexed.version)
  byte WITH_COMPRESSION_ID = 0x2;

  @JsonIgnore
  default Serializer getSerializer()
  {
    throw new UnsupportedOperationException("getSerializer");
  }

  @JsonIgnore
  Deserializer getDeserializer();

  interface Serializer
  {
    long getSerializedSize();

    long writeToChannel(WritableByteChannel channel) throws IOException;

    default Map<String, Object> getSerializeStats() { return null;}
  }

  interface Deserializer
  {
    void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException;
  }
}
