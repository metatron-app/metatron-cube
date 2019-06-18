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

import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 */
public class StringMetricSerde extends ComplexMetricSerde
{
  public static final StringMetricSerde INSTANCE = new StringMetricSerde();

  @Override
  public String getTypeName()
  {
    return ValueDesc.STRING_TYPE;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<?> extractedClass()
      {
        return String.class;
      }

      @Override
      public Object extractValue(Row inputRow, String metricName)
      {
        return Objects.toString(inputRow.getRaw(metricName), null);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    builder.setGenericColumn(
        new StringColumnPartSupplier(GenericIndexed.read(buffer, getObjectStrategy()))
    );
  }

  @Override
  public ObjectStrategy<String> getObjectStrategy()
  {
    return ObjectStrategy.STRING_STRATEGY;
  }
}
