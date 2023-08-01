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

import io.druid.collections.ResourceHolder;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.ByteBufferSerializer;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.SizePrefixedCompressedObjectStrategy;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public interface ComplexMetricSerde
{
  ValueDesc getType();

  default String getTypeName()
  {
    return getType().typeName();
  }

  default MetricExtractor getExtractor(List<String> typeHint)
  {
    return MetricExtractor.DUMMY;
  }

  /**
   * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the ComplexMetricSerde
   * to implement it's own versioning scheme to allow for changes of binary format in a forward-compatible manner.
   *  @param buffer  the buffer to deserialize
   * @param builder ColumnBuilder to add the column to
   * @return
   */
  @SuppressWarnings("unchecked")
  default ColumnBuilder deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    return builder.setComplexColumn(
        new NotCompressedComplexColumnPartSupplier(
            getType(),
            GenericIndexed.read(buffer, getObjectStrategy())
        )
    );
  }

  /**
   * This is deprecated because its usage is going to be removed from the code.
   * <p>
   * It was introduced before deserializeColumn() existed.  This method creates the assumption that Druid knows
   * how to interpret the actual column representation of the data, but I would much prefer that the ComplexMetricSerde
   * objects be in charge of creating and interpreting the whole column, which is what deserializeColumn lets
   * them do.
   *
   * @return an ObjectStrategy as used by GenericIndexed
   */
  ObjectStrategy getObjectStrategy();

  public static class Dummy implements ComplexMetricSerde
  {
    @Override
    public ValueDesc getType()
    {
      return ValueDesc.of("object");
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return ObjectStrategy.DUMMY;
    }
  }

  public static interface Factory
  {
    ComplexMetricSerde create(String[] elements);
  }

  abstract class CompressionSupport implements ComplexMetricSerde
  {
    @Override
    @SuppressWarnings("unchecked")
    public ColumnBuilder deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
    {
      final byte versionFromBuffer = buffer.get();
      if (versionFromBuffer == GenericIndexed.version) {
        GenericIndexed<?> indexed = GenericIndexed.readIndex(buffer, getObjectStrategy());
        builder.setType(ValueDesc.STRING)
               .setHasMultipleValues(false)
               .setComplexColumn(new NotCompressedComplexColumnPartSupplier(getType(), indexed));
      } else if (versionFromBuffer == ColumnPartSerde.WITH_COMPRESSION_ID) {
        CompressedObjectStrategy.CompressionStrategy compression = CompressedObjectStrategy.forId(buffer.get());
        ByteBuffer compressMeta = ByteBufferSerializer.prepareForRead(buffer);
        int[] mapping = new int[compressMeta.getInt()];
        for (int i = 0; i < mapping.length; i++) {
          mapping[i] = compressMeta.getInt();
        }
        SizePrefixedCompressedObjectStrategy strategy = new SizePrefixedCompressedObjectStrategy(compression);
        GenericIndexed<ResourceHolder<ByteBuffer>> compressed = GenericIndexed.read(buffer, strategy);
        builder.setType(ValueDesc.STRING)
               .setHasMultipleValues(false)
               .setComplexColumn(
                   new CompressedComplexColumnPartSupplier(compression, compressMeta, mapping, compressed, this)
               );
      } else {
        throw new IAE("Unknown version[%s]", versionFromBuffer);
      }
      return builder;
    }
  }
}
