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

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.druid.data.ParsingFail;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeName("json.stream")
public class StreamJsonInputRowParser implements InputRowParser.Streaming<Object>
{
  protected final TimestampSpec timestampSpec;
  protected final DimensionsSpec dimensionsSpec;
  protected final boolean ignoreInvalidRows;

  @JsonCreator
  public StreamJsonInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.timestampSpec = timestampSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.ignoreInvalidRows = ignoreInvalidRows;
  }

  @Override
  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @Override
  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new StreamJsonInputRowParser(
        timestampSpec,
        DimensionsSpec.withExclusions(dimensionsSpec, exclusions),
        ignoreInvalidRows
    );
  }

  @Override
  public Streaming<Object> withIgnoreInvalidRows(boolean ignoreInvalidRows)
  {
    return new StreamJsonInputRowParser(
        timestampSpec,
        dimensionsSpec,
        ignoreInvalidRows
    );
  }

  @Override
  public boolean accept(Object input)
  {
    return input instanceof InputStream || input instanceof Reader;
  }

  @Override
  public Iterator<InputRow> parseStream(Object input) throws IOException
  {
    final JsonParser parser = createParser(input);
    final List<String> dimensions = dimensionsSpec.getDimensionNames();
    return new Iterator<InputRow>()
    {
      private JsonToken token = parser.nextToken();

      @Override
      public boolean hasNext()
      {
        return !parser.isClosed() && token != null && token == JsonToken.START_OBJECT;
      }

      @Override
      public InputRow next()
      {
        Map<String, Object> event = null;
        try {
          event = parser.readValueAs(ObjectMappers.MAP_REF);
          token = parser.nextToken();
          Map<String, Object> merged = Rows.mergePartitions(event);
          DateTime dateTime = timestampSpec.extractTimestamp(merged);
          if (dateTime == null) {
            if (!ignoreInvalidRows) {
              throw ParsingFail.propagate(merged, new IAE("timestamp is null"));
            }
            return null;
          }
          return new MapBasedInputRow.Nested(dateTime, dimensions, merged);
        }
        catch (IOException e) {
          if (!ignoreInvalidRows) {
            throw ParsingFail.propagate(event, e);
          }
          return null;
        }
      }
    };
  }

  protected final JsonParser createParser(Object input) throws IOException
  {
    final JsonFactory factory = new DefaultObjectMapper().getFactory();
    if (input instanceof InputStream) {
      return factory.createParser((InputStream) input);
    } else if (input instanceof Reader) {
      return factory.createParser((Reader) input);
    } else {
      throw new ISE("never");
    }
  }
}
