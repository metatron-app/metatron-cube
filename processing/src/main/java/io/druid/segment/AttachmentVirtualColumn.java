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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;

import java.util.Objects;
import java.util.function.IntFunction;

/**
 */
@JsonTypeName("$attachment")
public class AttachmentVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x08;

  private final String outputName;
  private final ValueDesc columnType;

  @JsonCreator
  public AttachmentVirtualColumn(
      @JsonProperty("outputName") String outputName,
      @JsonProperty("columnType") ValueDesc columnType
  )
  {
    this.outputName = Preconditions.checkNotNull(outputName, "outputName should not be null");
    this.columnType = Preconditions.checkNotNull(columnType, "columnType should not be null");
  }

  @Override
  public ValueDesc resolveType(String column, TypeResolver types)
  {
    return column.equals(outputName) ? columnType : types.resolve(column);
  }

  @Override
  public ObjectColumnSelector asMetric(String column, ColumnSelectorFactory factory)
  {
    if (column.equals(outputName) && factory instanceof Cursor) {
      final Cursor cursor = (Cursor) factory;
      final IntFunction attachment = cursor.attachment(outputName);
      if (attachment != null) {
        return new ObjectColumnSelector()
        {
          @Override
          public Object get()
          {
            return attachment.apply(cursor.offset());
          }

          @Override
          public ValueDesc type()
          {
            return columnType;
          }
        };
      }
      return null;
    }

    return factory.makeObjectColumnSelector(outputName);
  }

  @Override
  public VirtualColumn duplicate()
  {
    return new AttachmentVirtualColumn(outputName, columnType);
  }

  @Override
  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @JsonProperty
  public ValueDesc getColumnType()
  {
    return columnType;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(VC_TYPE_ID)
                  .append(outputName)
                  .append(columnType);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AttachmentVirtualColumn)) {
      return false;
    }

    AttachmentVirtualColumn that = (AttachmentVirtualColumn) o;

    if (!Objects.equals(outputName, that.outputName)) {
      return false;
    }
    if (!Objects.equals(columnType, that.columnType)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(outputName, columnType);
  }

  @Override
  public String toString()
  {
    return "AttachmentVirtualColumn{" +
           "outputName='" + outputName + '\'' +
           ", columnType='" + columnType + '\'' +
           '}';
  }

}