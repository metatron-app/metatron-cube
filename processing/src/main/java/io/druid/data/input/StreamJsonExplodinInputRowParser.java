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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.ParsingFail;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.IAE;
import io.druid.utils.Runnables;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonTypeName("json.stream.explode")
public class StreamJsonExplodinInputRowParser extends StreamJsonInputRowParser
{
  @JsonCreator
  public StreamJsonExplodinInputRowParser(
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    super(timestampSpec, dimensionsSpec, ignoreInvalidRows);
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new StreamJsonExplodinInputRowParser(
        timestampSpec,
        DimensionsSpec.withExclusions(dimensionsSpec, exclusions),
        ignoreInvalidRows
    );
  }

  @Override
  public Streaming<Object> withIgnoreInvalidRows(boolean ignoreInvalidRows)
  {
    return new StreamJsonExplodinInputRowParser(
        timestampSpec,
        dimensionsSpec,
        ignoreInvalidRows
    );
  }

  @Override
  public Iterator<InputRow> parseStream(Object input) throws IOException
  {
    final JsonParser parser = createParser(input);
    if (parser.nextToken() == null) {
      return Collections.emptyIterator();
    }
    final List<String> dimensions = dimensionsSpec.getDimensionNames();
    return new Iterator<InputRow>()
    {
      private Iterator<InputRow> current = Collections.emptyIterator();

      @Override
      public boolean hasNext()
      {
        while (!current.hasNext() && !parser.isClosed() && parser.getCurrentToken() == JsonToken.START_OBJECT) {
          current = nextGroup();
        }
        return current.hasNext();
      }

      @Override
      public InputRow next()
      {
        return current.hasNext() ? current.next() : null;
      }

      private Iterator<InputRow> nextGroup()
      {
        Map<String, Object> node = null;
        try {
          node = parser.readValueAs(ObjectMappers.MAP_REF);
          final List<Runnable> exploding = iterate(node, Lists.newArrayList());
          final Map<String, Object> merged = Rows.mergePartitions(node);
          if (exploding.isEmpty()) {
            final DateTime dateTime = timestampSpec.extractTimestamp(merged);
            if (dateTime == null) {
              if (!ignoreInvalidRows) {
                throw ParsingFail.propagate(merged, new IAE("timestamp is null"));
              }
              return Iterators.emptyIterator();
            }
            return Arrays.<InputRow>asList(new MapBasedInputRow(dateTime, dimensions, merged)).iterator();
          }
          Iterator<InputRow> exploded = Iterators.transform(exploding.iterator(), (Runnable explode) -> {
            explode.run();
            final DateTime dateTime = timestampSpec.extractTimestamp(merged);
            if (dateTime == null) {
              if (!ignoreInvalidRows) {
                throw ParsingFail.propagate(merged, new IAE("timestamp is null"));
              }
              return null;
            }
            return new MapBasedInputRow(dateTime, dimensions, Maps.newHashMap(merged));
          });
          return Iterators.filter(exploded, Predicates.notNull());
        }
        catch (Exception e) {
          if (!ignoreInvalidRows) {
            throw ParsingFail.propagate(node, e);
          }
          return Collections.emptyIterator();
        }
      }

      @SuppressWarnings("unchecked")
      private List<Runnable> iterate(Map<String, Object> map, List<Runnable> current)
      {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
          final Object value = entry.getValue();
          if (value instanceof List) {
            final List list = (List) value;
            current = cartesian(current, asFunction(entry, list));
            for (Object element : list) {
              if (element instanceof Map) {
                current = iterate((Map) element, current);
              }
              // skip list of list
            }
          } else if (value instanceof Map) {
            current = iterate((Map) value, current);
          }
        }
        return current;
      }

      private Iterable<Runnable> asFunction(Map.Entry<String, Object> entry, List<Object> values)
      {
        return Iterables.transform(values, v -> () -> entry.setValue(v));
      }

      private List<Runnable> cartesian(List<Runnable> current, Iterable<Runnable> group)
      {
        if (current.isEmpty()) {
          Iterables.addAll(current, group);
          return current;
        }
        List<Runnable> exploded = Lists.newArrayList();
        List<Runnable> append = Lists.newArrayList(group);
        for (int i = 0; i < current.size(); i++) {
          for (int j = 0; j < append.size(); j++) {
            exploded.add(Runnables.andThen(current.get(i), append.get(j)));
          }
        }
        return exploded;
      }
    };
  }
}
