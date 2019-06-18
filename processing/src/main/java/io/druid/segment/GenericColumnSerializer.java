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

import io.druid.data.ValueDesc;
import io.druid.segment.column.ColumnDescriptor.Builder;
import io.druid.segment.data.HistogramBitmaps;
import io.druid.segment.data.MetricHistogram;
import io.druid.segment.serde.ColumnPartSerde;

import java.io.IOException;

public interface GenericColumnSerializer extends ColumnPartSerde.Serializer, MetricColumnSerializer
{
  int DEFAULT_NUM_SAMPLE = 40000;
  int DEFAULT_NUM_GROUP = 32;
  int DEFAULT_COMPACT_INTERVAL = 100000;

  public Builder buildDescriptor(ValueDesc desc, Builder builder) throws IOException;

  class FloatMinMax implements MetricHistogram.FloatType
  {
    float min = Float.MAX_VALUE;
    float max = Float.MIN_VALUE;
    int numZeros = 0;

    @Override
    public void offer(float value)
    {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
      if (value == 0) {
        numZeros++;
      }
    }

    @Override
    public float getMin()
    {
      return min;
    }

    @Override
    public float getMax()
    {
      return max;
    }

    @Override
    public int getNumZeros()
    {
      return numZeros;
    }

    @Override
    public HistogramBitmaps<Float> snapshot()
    {
      return null;
    }
  }

  class DoubleMinMax implements MetricHistogram.DoubleType
  {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    int numZeros = 0;

    @Override
    public void offer(double value)
    {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
      if (value == 0) {
        numZeros++;
      }
    }

    @Override
    public double getMin()
    {
      return min;
    }

    @Override
    public double getMax()
    {
      return max;
    }

    @Override
    public int getNumZeros()
    {
      return numZeros;
    }

    @Override
    public HistogramBitmaps<Double> snapshot()
    {
      return null;
    }
  }

  class LongMinMax implements MetricHistogram.LongType
  {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int numZeros = 0;

    @Override
    public void offer(long value)
    {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
      if (value == 0) {
        numZeros++;
      }
    }

    @Override
    public long getMin()
    {
      return min;
    }

    @Override
    public long getMax()
    {
      return max;
    }

    @Override
    public int getNumZeros()
    {
      return numZeros;
    }

    @Override
    public HistogramBitmaps<Long> snapshot()
    {
      return null;
    }
  }
}
