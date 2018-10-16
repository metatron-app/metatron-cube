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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.guava.nary.BinaryFn;
import io.druid.query.Result;

import java.util.Map;
import java.util.Objects;

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
      TypedSketch sketch = (TypedSketch) entry.getValue();
      TypedSketch merging = (TypedSketch) value2.get(entry.getKey());
      if (merging != null) {
        sketch = merge(entry.getKey(), sketch, merging);
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

  @SuppressWarnings("unchecked")
  final TypedSketch merge(String columnName, TypedSketch object1, TypedSketch object2)
  {
    Preconditions.checkArgument(
        Objects.equals(object1.type(), object2.type()),
        "Type mismatch.. " + object1.type() + " with " + object2.type() + " for column " + columnName
    );
    TypedSketch union = handler.newUnion(nomEntries, object1.type(), null);
    handler.updateWithSketch(union, object1.value());
    handler.updateWithSketch(union, object2.value());
    return handler.toSketch(union);
  }
}
