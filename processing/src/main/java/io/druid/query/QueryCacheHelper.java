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

package io.druid.query;

import com.google.common.collect.Lists;
import io.druid.common.Cacheable;
import io.druid.common.utils.StringUtils;
import io.druid.query.filter.DimFilterCacheHelper;

import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class QueryCacheHelper
{
  public static byte[] computeAggregatorBytes(List<? extends Cacheable> aggregatorSpecs)
  {
    if (aggregatorSpecs == null || aggregatorSpecs.isEmpty()) {
      return new byte[0];
    }

    List<byte[]> cacheKeySet = Lists.newArrayListWithCapacity(aggregatorSpecs.size());

    int totalSize = 0;
    for (Cacheable spec : aggregatorSpecs) {
      final byte[] cacheKey = computeCacheBytes(spec);
      cacheKeySet.add(cacheKey);
      totalSize += cacheKey.length;
    }

    ByteBuffer retVal = ByteBuffer.allocate(totalSize);
    for (byte[] bytes : cacheKeySet) {
      retVal.put(bytes);
    }
    return retVal.array();
  }

  public static byte[] computeCacheBytes(Cacheable cacheable)
  {
    return cacheable == null ? new byte[0] : cacheable.getCacheKey();
  }

  public static byte[] computeCacheBytes(List<String> strings)
  {
    if (strings == null || strings.isEmpty()) {
      return new byte[0];
    }
    int length = 0;
    byte[][] cache = new byte[strings.size()][];
    for (int i = 0; i < cache.length; i++) {
      cache[i] = StringUtils.toUtf8WithNullToEmpty(strings.get(i));
      length += cache[i].length;
    }
    ByteBuffer buffer = ByteBuffer.allocate(length + cache.length - 1);
    for (int i = 0; i < cache.length; i++) {
      if (buffer.position() > 0) {
        buffer.put(DimFilterCacheHelper.STRING_SEPARATOR);
      }
      buffer.put(cache[i]);
    }
    return buffer.array();
  }
}
