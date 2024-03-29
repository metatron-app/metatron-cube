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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ColumnDescriptor extends ColumnMeta
{
  public static long getPrefixedSize(List<ColumnDescriptor> descriptors)
  {
    long sum = 0;
    for (ColumnDescriptor descriptor : descriptors) {
      sum += Integer.BYTES;
      sum += descriptor.numBytes();
    }
    return sum;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  private final List<ColumnPartSerde> parts;

  @JsonCreator
  public ColumnDescriptor(
      @JsonProperty("valueType") ValueDesc valueType,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("parts") List<ColumnPartSerde> parts,
      @JsonProperty("descs") Map<String, String> descs,
      @JsonProperty("stats") Map<String, Object> stats
  )
  {
    super(valueType, hasMultipleValues, descs, stats);
    this.parts = Preconditions.checkNotNull(parts);
  }

  @JsonProperty
  public List<ColumnPartSerde> getParts()
  {
    return parts;
  }

  public long numBytes()
  {
    long retVal = 0;
    for (ColumnPartSerde part : parts) {
      retVal += part.getSerializer().getSerializedSize();
    }

    return retVal;
  }

  public long write(WritableByteChannel channel) throws IOException
  {
    long written = 0;
    for (ColumnPartSerde part : parts) {
      written += part.getSerializer().writeToChannel(channel);
    }
    return written;
  }

  public Column read(String columnName, ByteBuffer buffer, BitmapSerdeFactory serdeFactory) throws IOException
  {
    return read(new ColumnBuilder(columnName), buffer, serdeFactory).build();
  }

  public ColumnBuilder read(ColumnBuilder builder, ByteBuffer buffer, BitmapSerdeFactory serdeFactory) throws IOException
  {
    builder.setType(valueType)
           .setColumnStats(stats)
           .setColumnDescs(descs)
           .setHasMultipleValues(hasMultipleValues);

    for (ColumnPartSerde part : parts) {
      part.getDeserializer().read(buffer, builder, serdeFactory);
    }
    return builder;
  }

  public ColumnDescriptor withParts(List<ColumnPartSerde> parts)
  {
    return new ColumnDescriptor(valueType, hasMultipleValues, parts, descs, stats);
  }

  public ColumnPartSerde.Serializer asSerializer()
  {
    return new ColumnPartSerde.Serializer()
    {
      @Override
      public long getSerializedSize()
      {
        return numBytes();
      }

      @Override
      public long writeToChannel(WritableByteChannel channel) throws IOException
      {
        return write(channel);
      }
    };
  }

  public static class Builder
  {
    private ValueDesc valueType;
    private Boolean hasMultipleValues;

    private final List<ColumnPartSerde> parts = Lists.newArrayList();
    private final Map<String, String> descs = Maps.newLinkedHashMap();

    public Builder setValueType(ValueDesc type)
    {
      if (valueType != null && !valueType.equals(type)) {
        throw new IAE("valueType[%s] is already set, cannot change to[%s]", valueType, type);
      }
      this.valueType = type;
      return this;
    }

    public Builder setHasMultipleValues(boolean hasMultipleValues)
    {
      if (this.hasMultipleValues != null && this.hasMultipleValues != hasMultipleValues) {
        throw new IAE(
            "hasMultipleValues[%s] is already set, cannot change to[%s]", this.hasMultipleValues, hasMultipleValues
        );
      }
      this.hasMultipleValues = hasMultipleValues;
      return this;
    }

    public Builder addSerde(ColumnPartSerde serde)
    {
      parts.add(serde);
      return this;
    }

    public Builder addDescriptor(Map<String, String> descriptor)
    {
      if (descriptor != null) {
        descs.putAll(descriptor);
      }
      return this;
    }

    public ColumnDescriptor build()
    {
      return build(true);
    }

    public ColumnDescriptor build(boolean includeStats)
    {
      Preconditions.checkNotNull(valueType, "must specify a valueType");
      if (!includeStats) {
        return new ColumnDescriptor(valueType, hasMultipleValues != null && hasMultipleValues, parts, descs, null);
      }
      Set<String> statConflicts = Sets.newHashSet();
      Map<String, Object> stats = Maps.newHashMap();
      for (ColumnPartSerde part : parts) {
        final ColumnPartSerde.Serializer serializer = part.getSerializer();
        Map<String, Object> stat = serializer.getSerializeStats();
        if (stat == null) {
          continue;
        }
        statConflicts.addAll(Sets.intersection(stats.keySet(), stat.keySet()));
        stats.putAll(stat);
      }
      for (String conflict : statConflicts) {
        stats.remove(conflict);
      }
      return new ColumnDescriptor(valueType, hasMultipleValues != null && hasMultipleValues, parts, descs, stats);
    }
  }

  @Override
  public String toString()
  {
    return "ColumnDescriptor{" +
           "valueType=" + valueType +
           ", hasMultipleValues=" + hasMultipleValues +
           ", parts=" + parts +
           ", descs=" + descs +
           ", stats=" + stats +
           '}';
  }
}
