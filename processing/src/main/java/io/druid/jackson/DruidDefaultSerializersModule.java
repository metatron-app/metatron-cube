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

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.common.base.Throwables;
import io.druid.common.guava.BytesRef;
import io.druid.common.guava.HostAndPort;
import io.druid.common.guava.HostAndPortDeserializer;
import io.druid.common.utils.Sequences;
import io.druid.data.UTF8Bytes;
import io.druid.data.input.BulkRow;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;
import java.util.TimeZone;

/**
 */
public class DruidDefaultSerializersModule extends SimpleModule
{
  public DruidDefaultSerializersModule()
  {
    super("Druid default serializers");

    JodaStuff.register(this);

    addDeserializer(
        DateTimeZone.class,
        new JsonDeserializer<DateTimeZone>()
        {
          @Override
          public DateTimeZone deserialize(JsonParser jp, DeserializationContext ctxt)
              throws IOException
          {
            String tzId = jp.getText();
            try {
              return DateTimeZone.forID(tzId);
            } catch(IllegalArgumentException e) {
              // also support Java timezone strings
              return DateTimeZone.forTimeZone(TimeZone.getTimeZone(tzId));
            }
          }
        }
    );
    addSerializer(
        DateTimeZone.class,
        new JsonSerializer<DateTimeZone>()
        {
          @Override
          public void serialize(DateTimeZone dateTimeZone, JsonGenerator jsonGenerator, SerializerProvider provider)
              throws IOException
          {
            jsonGenerator.writeString(dateTimeZone.getID());
          }
        }
    );
    addSerializer(
        Sequence.class,
        new JsonSerializer<Sequence>()
        {
          @Override
          public void serialize(Sequence value, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            jgen.writeStartArray();
            value.accumulate(
                null,
                new Accumulator<Object, Object>()
                {
                  @Override
                  public Object accumulate(Object o, Object o1)
                  {
                    try {
                      jgen.writeObject(o1);
                    }
                    catch (IOException e) {
                      throw Throwables.propagate(e);
                    }
                    return null;
                  }
                }
            );
            jgen.writeEndArray();
          }
        }
    );
    addDeserializer(
        Sequence.class,
        new JsonDeserializer<Sequence>()
        {
          @Override
          public Sequence deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException
          {
            JavaType type = ctxt.constructType(List.class);
            List values = (List) ctxt.findContextualValueDeserializer(type, null)
                                     .deserialize(jp, ctxt);
            return Sequences.simple(values);
          }
        }
    );
    addSerializer(
        Yielder.class,
        new JsonSerializer<Yielder>()
        {
          @Override
          public void serialize(Yielder yielder, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            try {
              jgen.writeStartArray();
              while (!yielder.isDone()) {
                final Object o = yielder.get();
                jgen.writeObject(o);
                yielder = yielder.next(null);
              }
              jgen.writeEndArray();
            } finally {
              yielder.close();
            }
          }
        }
    );
    addSerializer(ByteOrder.class, ToStringSerializer.instance);
    addDeserializer(
        ByteOrder.class,
        new JsonDeserializer<ByteOrder>()
        {
          @Override
          public ByteOrder deserialize(JsonParser jp, DeserializationContext ctxt)
              throws IOException
          {
            if (ByteOrder.BIG_ENDIAN.toString().equals(jp.getText())) {
              return ByteOrder.BIG_ENDIAN;
            }
            return ByteOrder.LITTLE_ENDIAN;
          }
        }
    );
    addSerializer(
        UTF8Bytes.class,
        new JsonSerializer<UTF8Bytes>()
        {
          @Override
          public void serialize(UTF8Bytes utf8, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            final byte[] value = utf8.getValue();
            jgen.writeUTF8String(value, 0, value.length);
          }
        }
    );
    addSerializer(
        BytesRef.class,
        new JsonSerializer<BytesRef>()
        {
          @Override
          public void serialize(BytesRef utf8, final JsonGenerator jgen, SerializerProvider provider)
              throws IOException
          {
            jgen.writeBinary(utf8.bytes, 0, utf8.length);
          }
        }
    );
    addSerializer(
        MutableLong.class,
        new JsonSerializer<MutableLong>()
        {
          @Override
          public void serialize(MutableLong value, JsonGenerator jsonGenerator, SerializerProvider provider)
              throws IOException
          {
            jsonGenerator.writeNumber(value.longValue());
          }
        }
    );

    addSerializer(HostAndPort.class, ToStringSerializer.instance);
    addDeserializer(HostAndPort.class, HostAndPortDeserializer.std);

    addSerializer(BulkRow.class, BulkRow.SERIALIZER);

    addDeserializer(BulkRow.class, BulkRow.DESERIALIZER);
  }
}
