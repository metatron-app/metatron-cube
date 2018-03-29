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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import io.druid.data.ValueDesc;
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
public class ColumnDescriptor
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final ValueDesc valueType;
  private final boolean hasMultipleValues;
  private final List<ColumnPartSerde> parts;

  private Map<String, Object> stats;

  @JsonCreator
  public ColumnDescriptor(
      @JsonProperty("valueType") ValueDesc valueType,
      @JsonProperty("hasMultipleValues") boolean hasMultipleValues,
      @JsonProperty("parts") List<ColumnPartSerde> parts,
      @JsonProperty("stats") Map<String, Object> stats
  )
  {
    this.valueType = Preconditions.checkNotNull(valueType);
    this.hasMultipleValues = hasMultipleValues;
    this.parts = Preconditions.checkNotNull(parts);
    this.stats = stats;
  }

  @JsonProperty
  public ValueDesc getValueType()
  {
    return valueType;
  }

  @JsonProperty
  public boolean isHasMultipleValues()
  {
    return hasMultipleValues;
  }

  @JsonProperty
  public List<ColumnPartSerde> getParts()
  {
    return parts;
  }

  @JsonProperty
  public Map<String, Object> getStats()
  {
    return stats;
  }

  public void finalizeSerialization()
  {
    Set<String> conflicts = Sets.newHashSet();
    Map<String, Object> merged = Maps.newHashMap();
    for (ColumnPartSerde part : parts) {
      Map<String, Object> stat = part.getSerializer().getSerializeStats();
      if (stat == null) {
        continue;
      }
      conflicts.addAll(Sets.intersection(merged.keySet(), stat.keySet()));
      merged.putAll(stat);
    }
    for (String conflict : conflicts) {
      merged.remove(conflict);
    }
    this.stats = merged;
  }

  public long numBytes() throws IOException
  {
    long retVal = 0;

    for (ColumnPartSerde part : parts) {
      retVal += part.getSerializer().getSerializedSize();
    }

    return retVal;
  }

  public void write(WritableByteChannel channel) throws IOException
  {
    for (ColumnPartSerde part : parts) {
      part.getSerializer().writeToChannel(channel);
    }
  }

  public Column read(ByteBuffer buffer, BitmapSerdeFactory serdeFactory)
      throws IOException
  {
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(valueType)
        .setColumnStats(stats)
        .setHasMultipleValues(hasMultipleValues);

    for (ColumnPartSerde part : parts) {
      part.getDeserializer().read(buffer, builder, serdeFactory);
    }

    return builder.build();
  }

  public static class Builder
  {
    private ValueDesc valueType = null;
    private Boolean hasMultipleValues = null;

    private final List<ColumnPartSerde> parts = Lists.newArrayList();

    public Builder setValueType(ValueDesc valueType)
    {
      if (this.valueType != null && !this.valueType.equals(valueType)) {
        throw new IAE("valueType[%s] is already set, cannot change to[%s]", this.valueType, valueType);
      }
      this.valueType = valueType;
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

    public ColumnDescriptor build()
    {
      Preconditions.checkNotNull(valueType, "must specify a valueType");
      return new ColumnDescriptor(valueType, hasMultipleValues == null ? false : hasMultipleValues, parts, null);
    }
  }
}
