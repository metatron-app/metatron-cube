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

package io.druid.segment.data;

import io.druid.common.guava.BinaryRef;
import io.druid.data.input.BytesOutputStream;

import java.nio.ByteBuffer;

public interface ObjectStrategy<T>
{
  Class<? extends T> getClazz();

  default T fromByteBuffer(ByteBuffer buffer)
  {
    return fromByteBuffer(buffer, buffer.remaining());
  }

  default T fromByteBuffer(BinaryRef ref)
  {
    return fromByteBuffer(ref.toBuffer(), ref.length());
  }

  /**
   * Convert values from their underlying byte representation.
   *
   * Implementations of this method must not change the given buffer mark, or limit, but may modify its position.
   * Use buffer.asReadOnlyBuffer() or buffer.duplicate() if mark or limit need to be set.
   *
   * @param buffer buffer to read value from
   * @param numBytes number of bytes used to store the value, starting at buffer.position()
   * @return an object created from the given byte buffer representation
   */
  T fromByteBuffer(ByteBuffer buffer, int numBytes);

  byte[] toBytes(T val);

  interface CompareSupport<T> extends ObjectStrategy<T>, java.util.Comparator<T>
  {
  }

  // marker
  interface RawComparable<T> extends CompareSupport<T>
  {
  }

  interface SingleThreadSupport<T> extends ObjectStrategy<T>
  {
    ObjectStrategy<T> singleThreaded();
  }

  interface Recycling<T> extends ObjectStrategy<T>
  {
    T fromByteBuffer(ByteBuffer buffer, int numBytes, T object);
  }

  ObjectStrategy<String> STRING_STRATEGY = new ObjectStrategies.StringObjectStrategy();

  ObjectStrategy<Object> DUMMY = new ObjectStrategy<Object>()
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

  ObjectStrategy<byte[]> RAW = new ObjectStrategy<byte[]>()
  {
    @Override
    public Class<byte[]> getClazz()
    {
      return byte[].class;
    }

    @Override
    public byte[] fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int postion = buffer.position();
      byte[] bv = new byte[numBytes];
      for (int i = 0; i < bv.length; i++, postion++) {
        bv[i] = buffer.get(postion);
      }
      return bv;
    }

    @Override
    public byte[] toBytes(byte[] val)
    {
      return val;
    }
  };

  ObjectStrategy<float[]> FLOAT_ARRAY = new ObjectStrategy<float[]>()
  {
    @Override
    public Class<float[]> getClazz()
    {
      return float[].class;
    }

    @Override
    public float[] fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int postion = buffer.position();
      float[] fvs = new float[numBytes / Float.BYTES];
      for (int i = 0; i < fvs.length; i++, postion += Float.BYTES) {
        fvs[i] = buffer.getFloat(postion);
      }
      return fvs;
    }

    @Override
    public byte[] toBytes(float[] val)
    {
      BytesOutputStream bout = new BytesOutputStream(val.length * Float.BYTES);
      for (int i = 0; i < val.length; i++) {
        bout.writeFloat(val[i]);
      }
      return bout.toByteArray();
    }
  };

  ObjectStrategy<double[]> DOUBLE_ARRAY = new ObjectStrategy<double[]>()
  {
    @Override
    public Class<double[]> getClazz()
    {
      return double[].class;
    }

    @Override
    public double[] fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int postion = buffer.position();
      double[] fvs = new double[numBytes / Double.BYTES];
      for (int i = 0; i < fvs.length; i++, postion += Double.BYTES) {
        fvs[i] = buffer.getDouble(postion);
      }
      return fvs;
    }

    @Override
    public byte[] toBytes(double[] val)
    {
      BytesOutputStream bout = new BytesOutputStream(val.length * Double.BYTES);
      for (int i = 0; i < val.length; i++) {
        bout.writeDouble(val[i]);
      }
      return bout.toByteArray();
    }
  };

  ObjectStrategy<long[]> LONG_ARRAY = new ObjectStrategy<long[]>()
  {
    @Override
    public Class<long[]> getClazz()
    {
      return long[].class;
    }

    @Override
    public long[] fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      int postion = buffer.position();
      long[] fvs = new long[numBytes / Long.BYTES];
      for (int i = 0; i < fvs.length; i++, postion += Long.BYTES) {
        fvs[i] = buffer.getLong(postion);
      }
      return fvs;
    }

    @Override
    public byte[] toBytes(long[] val)
    {
      BytesOutputStream bout = new BytesOutputStream(val.length * Long.BYTES);
      for (int i = 0; i < val.length; i++) {
        bout.writeLong(val[i]);
      }
      return bout.toByteArray();
    }
  };
}
