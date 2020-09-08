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

public class DruidTDigestAggregator implements Aggregator.Simple<DruidTDigest>
{
  public static final Comparator<DruidTDigest> COMPARATOR = new Comparator<DruidTDigest>()
  {
    @Override
    public int compare(DruidTDigest o, DruidTDigest o1)
    {
      return Double.compare(o.median(), o1.median());
    }
  };

  // can hold compression * 5 points typically
  public static final int DEFAULT_COMPRESSION = 10;

  private final ObjectColumnSelector selector;
  private final double compression;

  public DruidTDigestAggregator(ObjectColumnSelector selector, int compression)
  {
    this.selector = selector;
    this.compression = compression;
  }

  @Override
  public DruidTDigest aggregate(DruidTDigest current)
  {
    final Object value = selector.get();
    if (value == null) {
      return current;
    }
    if (current == null) {
      current = new DruidTDigest(compression);
    }
    current.add(value);
    return current;
  }
}
