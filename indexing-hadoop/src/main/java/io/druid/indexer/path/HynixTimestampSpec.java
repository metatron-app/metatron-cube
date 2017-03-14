/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.data.input.impl.TimestampSpec;
import org.joda.time.DateTime;

import java.util.Map;

/**
 */
@JsonTypeName("hynix")
public class HynixTimestampSpec extends TimestampSpec
{
  @JsonCreator
  public HynixTimestampSpec(
      @JsonProperty("column") String timestampColumn,
      @JsonProperty("format") String format,
      // this value should never be set for production data
      @JsonProperty("missingValue") DateTime missingValue,
      @JsonProperty("invalidValue") DateTime invalidValue,
      @JsonProperty("replaceWrongColumn") boolean replaceWrongColumn,
      @JsonProperty("removeTimestampColumn") boolean removeTimestampColumn
  ) {
    super(timestampColumn, format, missingValue, invalidValue, replaceWrongColumn, removeTimestampColumn);
  }

  @Override
  public DateTime parseDateTime(Object input)
  {
    if (input == null) {
      Map<String, String> partition = HynixCombineInputFormat.CURRENT_PARTITION.get();
      if (partition != null) {
        input = partition.get(getTimestampColumn());
      }
    }
    return super.parseDateTime(input);
  }
}
