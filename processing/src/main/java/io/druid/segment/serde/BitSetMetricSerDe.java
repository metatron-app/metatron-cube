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

package io.druid.segment.serde;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.UOE;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

public class BitSetMetricSerDe implements ComplexMetricSerde
{
  @Override
  public ValueDesc getType()
  {
    return ValueDesc.BITSET;
  }

  @Override
  public MetricExtractor getExtractor(List<String> typeHint)
  {
    if (GuavaUtils.isNullOrEmpty(typeHint)) {
      return new MetricExtractor()
      {
        @Override
        public Object extract(Object value)
        {
          if (StringUtils.isNullOrEmpty(value)) {
            return null;
          }
          if (value instanceof BitSet) {
            return value;
          }
          final BitSet bitSet = new BitSet();
          if (value instanceof int[]) {
            final int[] indices = (int[]) value;
            for (int i = 0; i < indices.length; i++) {
              bitSet.set(indices[i]);
            }
          } else if (value instanceof List) {
            for (Object v : (List) value) {
              if (v != null) {
                bitSet.set(v instanceof Number
                           ? ((Number) v).intValue()
                           : Integer.parseInt(Objects.toString(v).trim()));
              }
            }
          } else {
            throw new IAE("Not supported type %s", value.getClass());
          }
          return bitSet;
        }
      };
    }
    String type = typeHint.get(0);
    if (type.equalsIgnoreCase("delimitedIntString")) {
      final String delimiter = typeHint.get(1);
      final Splitter splitter = delimiter.length() == 1 ? Splitter.on(delimiter.charAt(0)) : Splitter.on(delimiter);
      return new MetricExtractor()
      {
        @Override
        public Object extract(Object value)
        {
          if (StringUtils.isNullOrEmpty(value)) {
            return null;
          }
          final BitSet bitSet = new BitSet();
          for (String v : splitter.split(Objects.toString(value))) {
            bitSet.set(Integer.parseInt(v.trim()), true);
          }
          return bitSet;
        }
      };
    } else if (type.equalsIgnoreCase("prefixedBooleanColumns")) {
      return new MetricExtractor()
      {
        private final String prefix = typeHint.get(1);

        @Override
        public Object extractValue(Row inputRow, String metricName)
        {
          final BitSet bitSet = new BitSet();
          for (String column : Iterables.filter(inputRow.getColumns(), c -> c.startsWith(prefix))) {
            final Boolean bit = inputRow.getBoolean(column);
            if (bit == null) {
              return null;
            } else if (bit) {
              bitSet.set(Integer.parseInt(column.substring(prefix.length())), true);
            }
          }
          return bitSet;
        }

        @Override
        public Object extract(Object rawValue)
        {
          throw new UOE("cannot");
        }

        @Override
        public List<String> getExtractedNames(List<String> metricNames)
        {
          return Lists.newArrayList(Iterables.filter(metricNames, c -> c.startsWith(prefix)));
        }
      };
    }
    return null;
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<BitSet>()
    {
      @Override
      public Class<BitSet> getClazz()
      {
        return BitSet.class;
      }

      @Override
      public BitSet fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        if (numBytes == 0) {
          return null;
        }
        ByteBuffer readOnly = buffer.asReadOnlyBuffer();
        readOnly.limit(readOnly.position() + numBytes);
        return BitSet.valueOf(readOnly);
      }

      @Override
      public byte[] toBytes(BitSet val)
      {
        return val == null ? StringUtils.EMPTY_BYTES : val.toByteArray();
      }
    };
  }
}
