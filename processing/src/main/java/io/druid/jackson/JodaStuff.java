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

package io.druid.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.joda.deser.DurationDeserializer;
import com.fasterxml.jackson.datatype.joda.deser.PeriodDeserializer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;

/**
 */
public class JodaStuff
{
  public static ObjectMapper overrideForClient(ObjectMapper mapper)
  {
    ObjectMapper client = mapper.copy();
    SimpleSerializers serializers = new SimpleSerializers();
    serializers.addSerializer(DateTime.class, ToStringSerializer.instance);
    return client.setSerializerFactory(mapper.getSerializerFactory().withAdditionalSerializers(serializers));
  }

  @SuppressWarnings("unchecked")
  static SimpleModule register(SimpleModule module)
  {
    module.addKeyDeserializer(DateTime.class, new DateTimeKeyDeserializer());
    module.addDeserializer(DateTime.class, new DateTimeDeserializer());
    module.addSerializer(DateTime.class, new DateTimeSerializer());
    module.addDeserializer(Interval.class, new JodaStuff.IntervalDeserializer());
    module.addSerializer(Interval.class, ToStringSerializer.instance);
    JsonDeserializer<?> periodDeserializer = new PeriodDeserializer();
    module.addDeserializer(Period.class, (JsonDeserializer<Period>) periodDeserializer);
    module.addSerializer(Period.class, ToStringSerializer.instance);
    module.addDeserializer(Duration.class, new DurationDeserializer());
    module.addSerializer(Duration.class, ToStringSerializer.instance);

    return module;
  }

  /**
   */
  private static class IntervalDeserializer extends StdDeserializer<Interval>
  {
    public IntervalDeserializer()
    {
      super(Interval.class);
    }

    @Override
    public Interval deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException
    {
      return new Interval(jsonParser.getText());
    }
  }

  private static class DateTimeKeyDeserializer extends KeyDeserializer
  {
    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException
    {
      return new DateTime(key);
    }
  }

  private static class DateTimeSerializer extends StdSerializer<DateTime>
  {
    protected DateTimeSerializer()
    {
      super(DateTime.class);
    }

    @Override
    public void serialize(DateTime value, JsonGenerator jgen, SerializerProvider provider) throws IOException
    {
      DateTimeZone timeZone = value.getChronology().getZone();
      jgen.writeStartObject();
      jgen.writeNumberField("m", value.getMillis());
      if (timeZone != DateTimeZone.UTC) {
        jgen.writeStringField("z", timeZone.getID());
      }
      jgen.writeEndObject();
    }
  }

  private static class DateTimeDeserializer extends StdDeserializer<DateTime>
  {
    public DateTimeDeserializer()
    {
      super(DateTime.class);
    }

    @Override
    public DateTime deserialize(JsonParser jp, DeserializationContext ctxt)
        throws IOException, JsonProcessingException
    {
      JsonToken t = jp.getCurrentToken();
      if (t == JsonToken.VALUE_NUMBER_INT) {
        return new DateTime(jp.getLongValue());
      }
      if (t == JsonToken.VALUE_STRING) {
        String str = jp.getText().trim();
        if (str.length() == 0) { // [JACKSON-360]
          return null;
        }
        // make sure to preserve time zone information when parsing timestamps
        return ISODateTimeFormat.dateTimeParser()
                                .withOffsetParsed()
                                .parseDateTime(str);
      }
      if (t == JsonToken.START_OBJECT) {
        long millis = 0;
        DateTimeZone timeZone = DateTimeZone.UTC;
        for (t = jp.nextToken(); t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
          String propName = jp.getCurrentName();
          // Skip field name:
          jp.nextToken();
          if (propName.equals("m")) {
            millis = jp.getLongValue();
          } else {
            timeZone = DateTimeZone.forID(jp.getValueAsString());
          }
        }
        return new DateTime(millis, timeZone);
      }
      throw ctxt.mappingException(handledType());
    }
  }
}
