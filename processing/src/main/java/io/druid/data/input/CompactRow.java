/*
* Licensed to Metamarkets Group Inc. (Metamarkets) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. Metamarkets licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;
import io.druid.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Collection;

/**
 */
public class CompactRow extends AbstractRow
{
  private final Object[] values;

  @JsonCreator
  public CompactRow(@JsonProperty("values") Object[] values)
  {
    this.values = values;
  }

  @JsonProperty
  public Object[] getValues()
  {
    return values;
  }

  @Override
  public long getTimestampFromEpoch()
  {
    return ((Number) values[0]).longValue();
  }

  @Override
  public DateTime getTimestamp()
  {
    return DateTimes.utc(getTimestampFromEpoch());
  }

  @Override
  public int compareTo(Row o)
  {
    return Longs.compare(getTimestampFromEpoch(), o.getTimestampFromEpoch());
  }

  @Override
  public Object getRaw(String dimension)
  {
    throw new UnsupportedOperationException("getRaw");
  }

  @Override
  public Collection<String> getColumns()
  {
    throw new UnsupportedOperationException("getColumns");
  }

  @Override
  public String toString()
  {
    return Arrays.toString(values);
  }

  @Override
  public boolean equals(Object other)
  {
    return Arrays.equals(values, ((CompactRow) other).values);
  }
}
