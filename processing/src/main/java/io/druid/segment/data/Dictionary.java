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

package io.druid.segment.data;

import io.druid.segment.ColumnPartProvider;
import io.druid.segment.serde.ColumnPartSerde;

import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.druid.segment.data.DictionaryCompareOp.EQ;
import static io.druid.segment.data.DictionaryCompareOp.GT;
import static io.druid.segment.data.DictionaryCompareOp.GTE;
import static io.druid.segment.data.DictionaryCompareOp.LT;
import static io.druid.segment.data.DictionaryCompareOp.LTE;
import static io.druid.segment.data.DictionaryCompareOp.NEQ;

// common interface of non-compressed(GenericIndexed) and compressed dictionary
public interface Dictionary<T> extends Indexed.Searchable<T>
{
  int flag();

  default boolean isSorted()
  {
    return Feature.SORTED.isSet(flag());
  }

  default Dictionary<T> dedicated() {return this;}

  default Stream<T> stream()
  {
    Dictionary<T> dedicated = dedicated();
    return IntStream.range(0, size()).mapToObj(dedicated::get);
  }

  Boolean containsNull();     // null for unknown

  long getSerializedSize();

  void close();

  static enum Feature
  {
    SORTED,
    VSIZED_VALUE,
    NO_OFFSET;

    public final int mask = 1 << ordinal();

    public boolean isSet(int flags) {return (mask & flags) != 0;}

    public int set(int flags, boolean v)
    {
      return v ? flags | mask : flags & ~mask;
    }

    public int getMask() {return mask;}
  }

  static DictionaryCompareOp compareOp(String name)
  {
    switch (name) {
      case "==": return EQ;
      case "!=": return NEQ;
      case ">":  return GT;
      case ">=": return GTE;
      case "<":  return LT;
      case "<=": return LTE;
    }
    return null;
  }

  static ColumnPartSerde.Deserializer asDeserializer(Dictionary<String> dictionary)
  {
    return (buffer, builder, factory) -> builder.setDictionary(asProvider(dictionary));
  }

  static <T> ColumnPartProvider<Dictionary<T>> asProvider(Dictionary<T> dictionary)
  {
    return new ColumnPartProvider<Dictionary<T>>()
    {
      @Override
      public int numRows()
      {
        return dictionary.size();
      }

      @Override
      public long getSerializedSize()
      {
        return dictionary.getSerializedSize();
      }

      @Override
      @SuppressWarnings("unchecked")
      public Class provides()
      {
        return dictionary.getClass();
      }

      @Override
      public Dictionary<T> get()
      {
        return dictionary.dedicated();
      }
    };
  }
}
