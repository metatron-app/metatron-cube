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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;
import org.apache.lucene.document.Field;

/**
 */
public class TextIndexingStrategy implements LuceneIndexingStrategy
{
  private final String fieldName;

  @JsonCreator
  public TextIndexingStrategy(@JsonProperty("fieldName") String fieldName)
  {
    this.fieldName = Preconditions.checkNotNull(fieldName);
  }

  @Override
  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String getFieldDescriptor()
  {
    return "text";
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    if (type.isPrimitive()) {
      return Lucenes.makeTextFieldGenerator(fieldName);
    }
    if (type.isStruct()) {
      StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));
      final int index = serde.indexOf(fieldName);
      Preconditions.checkArgument(index >= 0, "invalid field name " + fieldName);
      return Functions.compose(
          Lucenes.makeTextFieldGenerator(fieldName),
          new Function<Object, Object>()
          {
            @Override
            public Object apply(Object input)
            {
              return ((Object[]) input)[index];
            }
          }
      );
    }
    throw new IllegalArgumentException("cannot index " + type + " as text");
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TextIndexingStrategy that = (TextIndexingStrategy) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return fieldName.hashCode();
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
