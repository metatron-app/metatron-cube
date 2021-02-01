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

package io.druid.query.aggregation;

import com.google.common.base.Preconditions;
import io.druid.common.utils.StringUtils;
import io.druid.data.input.Row;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;

/**
 */
public class DecimalMetricSerde implements ComplexMetricSerde
{
  public static final int DEFAULT_PRECISION = 18;
  public static final int DEFAULT_SCALE = 0;
  public static final RoundingMode DEFAULT_ROUNDING_MODE = RoundingMode.DOWN;

  private final MathContext context;
  private final int scale;
  private final String typeName;

  public DecimalMetricSerde(int precision, int scale, RoundingMode roundingMode)
  {
    Preconditions.checkArgument(scale >= 0 && scale <= 30, "out of range %d", scale);
    Preconditions.checkArgument(precision == 0 || (precision > 0 && precision > scale), "out of range %d", scale);
    this.scale = scale;
    this.context = new MathContext(precision, roundingMode);
    this.typeName = "decimal(" + precision + "," + scale + "," + roundingMode + ")";
  }

  public DecimalMetricSerde(int precision, int scale)
  {
    this(precision, scale, DEFAULT_ROUNDING_MODE);
  }

  public DecimalMetricSerde()
  {
    this(DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_ROUNDING_MODE);
  }

  public int precision()
  {
    return context.getPrecision();
  }

  public int scale()
  {
    return scale;
  }

  public RoundingMode roundingMode()
  {
    return context.getRoundingMode();
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {

      @Override
      public Object extractValue(Row inputRow, String metricName)
      {
        Object raw = inputRow.getRaw(metricName);
        if (raw == null) {
          return null;
        }
        BigDecimal decimal;
        if (raw instanceof BigDecimal) {
          decimal = ((BigDecimal) raw);
        } else if (raw instanceof Number) {
          if (raw instanceof Double || raw instanceof Float) {
            decimal = new BigDecimal(((Number) raw).doubleValue());
          } else {
            decimal = new BigDecimal(((Number) raw).longValue());
          }
        } else if (raw instanceof String) {
          decimal = new BigDecimal((String) raw);
        } else {
          throw new IllegalArgumentException("unsupported type " + raw.getClass());
        }
        return decimal.plus(context).setScale(scale, context.getRoundingMode());
      }
    };
  }

  @Override
  public ObjectStrategy<BigDecimal> getObjectStrategy()
  {
    return new ObjectStrategy<BigDecimal>()
    {
      @Override
      public Class<BigDecimal> getClazz()
      {
        return BigDecimal.class;
      }

      @Override
      public BigDecimal fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        byte[] bytes = new byte[numBytes];
        buffer.get(bytes, 0, numBytes);
        return new BigDecimal(new BigInteger(bytes), scale);
      }

      @Override
      public byte[] toBytes(BigDecimal val)
      {
        if (val == null) {
          return StringUtils.EMPTY_BYTES;
        }
        BigDecimal normalized = val.plus(context).setScale(scale, context.getRoundingMode());
        return normalized.unscaledValue().toByteArray();
      }
    };
  }

  @Override
  public String toString()
  {
    return "DecimalMetricSerde{" +
           "precision=" + context.getPrecision() +
           ", scale=" + scale +
           ", rounding=" + context.getRoundingMode() +
           "}";
  }

  public static class Factory implements ComplexMetricSerde.Factory
  {
    @Override
    public ComplexMetricSerde create(String[] elements)
    {
      if (elements == null || elements.length <= 1) {
        return new DecimalMetricSerde(DEFAULT_PRECISION, DEFAULT_SCALE, DEFAULT_ROUNDING_MODE);
      }
      int precision = Integer.valueOf(elements[1]);
      int scale = elements.length > 2 ? Integer.valueOf(elements[2]) : DEFAULT_SCALE;
      RoundingMode roundingMode = elements.length > 3
                                  ? RoundingMode.valueOf(elements[3].toUpperCase())
                                  : DEFAULT_ROUNDING_MODE;

      return new DecimalMetricSerde(precision, scale, roundingMode);
    }
  }
}
