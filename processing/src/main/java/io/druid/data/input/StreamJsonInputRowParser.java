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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.druid.data.ParsingFail;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.jackson.DefaultObjectMapper;
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
  private final TimestampSpec timestampSpec;
  private final DimensionsSpec dimensionsSpec;
  private final boolean ignoreInvalidRows;

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
        return !parser.isClosed() && token == JsonToken.START_OBJECT;
      }

      @Override
      public InputRow next()
      {
        JsonNode node = null;
        try {
          node = parser.readValueAs(JsonNode.class);
          token = parser.nextToken();
          Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
          final Map<String, Object> event = Maps.newHashMap();
          while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode value = field.getValue();
            if (value.isTextual()) {
              event.put(field.getKey(), value.textValue());
            } else if (value.isNumber()) {
              event.put(field.getKey(), value.numberValue());
            } else if (value.isBoolean()) {
              event.put(field.getKey(), value.booleanValue());
            }
          }
          Map<String, Object> merged = Rows.mergePartitions(event);
          DateTime dateTime = Preconditions.checkNotNull(timestampSpec.extractTimestamp(merged));
          return new MapBasedInputRow(dateTime, dimensions, merged);
        }
        catch (IOException e) {
          if (!ignoreInvalidRows) {
            throw ParsingFail.propagate(node, e);
          }
          return null;
        }
      }
    };
  }

  private JsonParser createParser(Object input) throws IOException
  {
    JsonFactory factory = new DefaultObjectMapper().getFactory();
    if (input instanceof InputStream) {
      return factory.createParser((InputStream) input);
    } else if (input instanceof Reader) {
      return factory.createParser((Reader) input);
    } else {
      throw new ISE("never");
    }
  }
}
