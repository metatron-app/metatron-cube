/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.BitSlicedBitmaps;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ByteBufferSerializer;
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
    @JsonSubTypes.Type(name = "string", value = StringGenericColumnPartSerde.class),
    @JsonSubTypes.Type(name = "stringDictionary", value = DictionaryEncodedColumnPartSerde.class),
    @JsonSubTypes.Type(name = "lucene", value = ComplexColumnSerializer.LuceneIndexPartSerDe.class),
    @JsonSubTypes.Type(name = "histogram", value = HistogramBitmaps.SerDe.class),
    @JsonSubTypes.Type(name = "bsb", value = BitSlicedBitmaps.SerDe.class)
})
public interface ColumnPartSerde
{
  public Serializer getSerializer();

  public Deserializer getDeserializer();

  public interface Serializer
  {
    public long getSerializedSize() throws IOException;

    public void writeToChannel(WritableByteChannel channel) throws IOException;

    public Map<String, Object> getSerializeStats();

    public static abstract class Abstract implements Serializer
    {
      @Override
      public final Map<String, Object> getSerializeStats()
      {
        return null;
      }
    }
  }

  public interface Deserializer
  {
    public void read(
        ByteBuffer buffer,
        ColumnBuilder builder,
        BitmapSerdeFactory serdeFactory
    ) throws IOException;
  }

  public class Skipper implements ColumnPartSerde
  {
    @Override
    public Serializer getSerializer()
    {
      throw new IllegalStateException("not for use");
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(
            ByteBuffer buffer,
            ColumnBuilder builder,
            BitmapSerdeFactory serdeFactory
        ) throws IOException
        {
          ByteBufferSerializer.prepareForRead(buffer);  // skip this part
        }
      };
    }
  }
}
