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

package io.druid.query.aggregation.median;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class DruidTDigestAggregator implements Aggregator
{
  public static final Comparator COMPARATOR = new Comparator()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Double.compare(((DruidTDigest) o).median(), ((DruidTDigest) o1).median());
    }
  };

  // can hold compression * 5 points typically
  public static final int DEFAULT_COMPRESSION = 10;

  private final ObjectColumnSelector selector;
  private final double compression;
  private DruidTDigest digest;

  public DruidTDigestAggregator(
      ObjectColumnSelector selector,
      int compression
  )
  {
    this.digest = new DruidTDigest(compression);
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public void aggregate()
  {
    digest.add(selector.get());
  }

  @Override
  public void reset()
  {
    this.digest = new DruidTDigest(compression);
  }

  @Override
  public Object get()
  {
    return digest;
  }

  @Override
  public Float getFloat()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getFloat()");
  }

  @Override
  public Long getLong()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getLong()");
  }

  @Override
  public Double getDouble()
  {
    throw new UnsupportedOperationException("DruidTDigestAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  public int getMaxStorageSize()
  {
    return digest.maxByteSize();
  }
}
