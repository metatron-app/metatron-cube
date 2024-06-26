/*
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

package io.druid.java.util.common.parsers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Class that can parseToMap Strings into Maps.
 */
public interface Parser<K, V>
{
  /**
   * Parse a String into a Map.
   *
   * @throws ParseException if the String cannot be parsed
   */
  @Nullable
  Map<K, V> parseToMap(String input);

  /**
   * Set the fieldNames that you expect to see in parsed Maps. Deprecated; Parsers should not, in general, be
   * expected to know what fields they will return. Some individual types of parsers do need to know (like a TSV
   * parser) and those parsers have their own way of setting field names.
   */
  @Deprecated
  public void setFieldNames(Iterable<String> fieldNames);

  /**
   * Returns the fieldNames that we expect to see in parsed Maps, if known, or null otherwise. Deprecated; Parsers
   * should not, in general, be expected to know what fields they will return.
   */
  @Deprecated
  public List<String> getFieldNames();
}
