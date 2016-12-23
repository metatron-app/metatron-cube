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

package io.druid.query.sketch;

import com.google.common.collect.Maps;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.query.Result;

import java.util.Map;

/**
 */
public class SketchBinaryFn
    implements BinaryFn<Result<Map<String, Object>>, Result<Map<String, Object>>, Result<Map<String, Object>>>
{
  private final int nomEntries;
  private final SketchHandler handler;

  public SketchBinaryFn(int nomEntries, SketchHandler handler)
  {
    this.nomEntries = nomEntries;
    this.handler = handler;
  }

  @Override
  public Result<Map<String, Object>> apply(
      Result<Map<String, Object>> arg1, Result<Map<String, Object>> arg2
  )
  {
    if (arg2 == null) {
      return arg1;
    }
    final Map<String, Object> value1 = arg1.getValue();
    final Map<String, Object> value2 = arg2.getValue();

    final Map<String, Object> merged = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : value1.entrySet()) {
      Object sketch = entry.getValue();
      Object merging = value2.get(entry.getKey());
      if (merging != null) {
        sketch = merge(sketch, merging);
      }
      merged.put(entry.getKey(), sketch);
    }
    for (Map.Entry<String, Object> entry : value2.entrySet()) {
      if (!value1.containsKey(entry.getKey())) {
        merged.put(entry.getKey(), entry.getValue());
      }
    }
    return new Result<>(arg1.getTimestamp(), merged);
  }

  private Object merge(Object object1, Object object2)
  {
    Object union = handler.newUnion(nomEntries);
    handler.updateWithSketch(union, object1);
    handler.updateWithSketch(union, object2);
    return handler.toSketch(union);
  }
}
