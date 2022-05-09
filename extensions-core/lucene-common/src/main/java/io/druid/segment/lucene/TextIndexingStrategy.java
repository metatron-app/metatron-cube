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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.StructMetricSerde;
import org.apache.lucene.document.Field;

import java.util.Objects;

/**
 */
@JsonTypeName("text")
public class TextIndexingStrategy implements LuceneIndexingStrategy
{
  private final String fieldName;

  @JsonCreator
  public TextIndexingStrategy(@JsonProperty("fieldName") String fieldName)
  {
    this.fieldName = fieldName;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String getFieldDescriptor()
  {
    return TEXT_DESC;
  }

  @Override
  public LuceneIndexingStrategy withFieldName(String fieldName)
  {
    return new TextIndexingStrategy(fieldName);
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    if (type.isPrimitive()) {
      return Lucenes.makeTextFieldGenerator(fieldName);
    }
    if (type.isStruct()) {
      final ComplexMetricSerde serde = Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));
      final int index = ((StructMetricSerde) serde).indexOf(fieldName);
      Preconditions.checkArgument(index >= 0, "invalid field name %s", fieldName);
      return Functions.compose(
          Lucenes.makeTextFieldGenerator(fieldName), input -> ((Object[]) input)[index]
      );
    }
    throw new IAE("cannot index %s as text", type);
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

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName);
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
