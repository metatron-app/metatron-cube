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

package io.druid.query.sketch;

import io.druid.data.ValueDesc;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;

import java.nio.ByteBuffer;

public class TypedSketchMetricSerDes
{
  public static class Theta extends ComplexMetricSerde.CompressionSupport
  {
    @Override
    public ValueDesc getType()
    {
      return TypedSketch.THETA;
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<TypedSketch>()
      {
        @Override
        public Class<TypedSketch> getClazz()
        {
          return TypedSketch.class;
        }

        @Override
        public TypedSketch fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          return TypedSketch.deserialize(SketchOp.THETA, buffer, null);
        }

        @Override
        public byte[] toBytes(TypedSketch sketch)
        {
          return sketch.toByteArray();
        }
      };
    }
  }

  public static class Quantile extends ComplexMetricSerde.CompressionSupport
  {
    @Override
    public ValueDesc getType()
    {
      return TypedSketch.QUANTILE;
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<TypedSketch>()
      {
        @Override
        public Class<TypedSketch> getClazz()
        {
          return TypedSketch.class;
        }

        @Override
        public TypedSketch fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          return TypedSketch.deserialize(SketchOp.QUANTILE, buffer, null);
        }

        @Override
        public byte[] toBytes(TypedSketch sketch)
        {
          return sketch.toByteArray();
        }
      };
    }
  }

  public static class Frequency extends ComplexMetricSerde.CompressionSupport
  {
    @Override
    public ValueDesc getType()
    {
      return TypedSketch.FREQUENCY;
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<TypedSketch>()
      {
        @Override
        public Class<TypedSketch> getClazz()
        {
          return TypedSketch.class;
        }

        @Override
        public TypedSketch fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          return TypedSketch.deserialize(SketchOp.FREQUENCY, buffer, null);
        }

        @Override
        public byte[] toBytes(TypedSketch sketch)
        {
          return sketch.toByteArray();
        }
      };
    }
  }

  public static class Sampling extends ComplexMetricSerde.CompressionSupport
  {
    @Override
    public ValueDesc getType()
    {
      return TypedSketch.SAMPLING;
    }

    @Override
    public ObjectStrategy getObjectStrategy()
    {
      return new ObjectStrategy<TypedSketch>()
      {
        @Override
        public Class<TypedSketch> getClazz()
        {
          return TypedSketch.class;
        }

        @Override
        public TypedSketch fromByteBuffer(ByteBuffer buffer, int numBytes)
        {
          return TypedSketch.deserialize(SketchOp.SAMPLING, buffer, null);
        }

        @Override
        public byte[] toBytes(TypedSketch sketch)
        {
          return sketch.toByteArray();
        }
      };
    }
  }
}
