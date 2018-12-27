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

package io.druid.segment;

import com.google.common.collect.Lists;
import io.druid.segment.column.Column;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.data.Indexed;
import org.apache.commons.io.IOUtils;

import java.util.List;

/**
 */
public class Segments
{
  public static Indexed<String> getAvailableDimensions(Segment segment)
  {
    if (segment.asQueryableIndex(false) != null) {
      return segment.asQueryableIndex(false).getAvailableDimensions();
    }
    return segment.asStorageAdapter(false).getAvailableDimensions();
  }

  public static List<DictionaryEncodedColumn> findDictionaryWithSketch(List<Segment> segments, String columnName)
  {
    List<DictionaryEncodedColumn> found = Lists.newArrayList();
    for (Segment segment : Lists.reverse(segments)) {
      QueryableIndex index = segment.asQueryableIndex(false);
      if (index != null) {
        Column column = index.getColumn(columnName);
        if (column != null) {
          DictionaryEncodedColumn dictionary = column.getDictionaryEncoding();
          if (dictionary != null && dictionary.hasSketch()) {
            found.add(dictionary);
          } else {
            IOUtils.closeQuietly(dictionary);
          }
        }
      }
    }
    return found;
  }

  public static int getNumRows(Segment segment)
  {
    if (segment.asQueryableIndex(false) != null) {
      return segment.asQueryableIndex(false).getNumRows();
    }
    return segment.asStorageAdapter(false).getNumRows();
  }
}
