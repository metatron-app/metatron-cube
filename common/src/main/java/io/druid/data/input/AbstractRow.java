/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;

/**
 */
public abstract class AbstractRow implements Row
{
  private static final Function<Object, String> TO_STRING_INCLUDING_NULL = new Function<Object, String>()
  {
    @Override
    public String apply(final Object o)
    {
      return String.valueOf(o);
    }
  };

  @Override
  public float getFloatMetric(String metric)
  {
    final Float value = getFloat(metric);
    return value == null ? 0F : value;
  }

  @Override
  public double getDoubleMetric(String metric)
  {
    final Double value = getDouble(metric);
    return value == null ? 0D : value;
  }

  @Override
  public long getLongMetric(String metric)
  {
    final Long value = getLong(metric);
    return value == null ? 0L : value;
  }

  @Override
  public List<String> getDimension(String dimension)
  {
    final Object dimValue = getRaw(dimension);

    if (dimValue == null) {
      return Collections.emptyList();
    } else if (dimValue instanceof List) {
      // guava's toString function fails on null objects, so please do not use it
      return Lists.transform(
          (List) dimValue,
          TO_STRING_INCLUDING_NULL
      );
    } else {
      return Collections.singletonList(String.valueOf(dimValue));
    }
  }

  @Override
  public Boolean getBoolean(String metric)
  {
    return Rows.parseBoolean(getRaw(metric));
  }

  @Override
  public Float getFloat(String metric)
  {
    return Rows.parseFloat(getRaw(metric));
  }

  @Override
  public Double getDouble(String metric)
  {
    return Rows.parseDouble(getRaw(metric));
  }

  @Override
  public Long getLong(String metric)
  {
    return Rows.parseLong(getRaw(metric));
  }

  @Override
  public String getString(String metric)
  {
    return Rows.parseString(getRaw(metric));
  }

  @Override
  public long getTimestampFromEpoch()
  {
    throw new UnsupportedOperationException("getTimestampFromEpoch");
  }

  @Override
  public DateTime getTimestamp()
  {
    throw new UnsupportedOperationException("getTimestamp");
  }

  @Override
  public int compareTo(Row o)
  {
    throw new UnsupportedOperationException("compareTo");
  }
}
