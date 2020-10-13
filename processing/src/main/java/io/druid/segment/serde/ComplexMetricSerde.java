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

import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

/**
 */
public abstract class ComplexMetricSerde
{
  public abstract String getTypeName();

  public ComplexMetricExtractor getExtractor()
  {
    return ComplexMetricExtractor.DUMMY;
  }

  /**
   * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the ComplexMetricSerde
   * to implement it's own versioning scheme to allow for changes of binary format in a forward-compatible manner.
   *
   * @param buffer the buffer to deserialize
   * @param builder ColumnBuilder to add the column to
   */
  @SuppressWarnings("unchecked")
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    builder.setComplexColumn(
        new ComplexColumnPartSupplier(
            getTypeName(),
            GenericIndexed.read(buffer, getObjectStrategy())
        )
    );
  }

  /**
   * This is deprecated because its usage is going to be removed from the code.
   *
   * It was introduced before deserializeColumn() existed.  This method creates the assumption that Druid knows
   * how to interpret the actual column representation of the data, but I would much prefer that the ComplexMetricSerde
   * objects be in charge of creating and interpreting the whole column, which is what deserializeColumn lets
   * them do.
   *
   * @return an ObjectStrategy as used by GenericIndexed
   */
  public abstract ObjectStrategy getObjectStrategy();

  public static class Dummy extends ComplexMetricSerde
  {
    @Override
    public String getTypeName()
    {
      return "object";
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<Object>()
      {
        @Override
        public Class getClazz()
        {
          return Object.class;
        }

        @Override
        public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          throw new UnsupportedOperationException("fromByteBuffer");
        }

        @Override
        public byte[] toBytes(Object val)
        {
          throw new UnsupportedOperationException("toBytes");
        }
      };
    }
  }

  public static interface Factory
  {
    ComplexMetricSerde create(String[] elements);
  }
}
