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

package io.druid.query.sketch;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.guava.nary.BinaryFn;

import java.util.Objects;

/**
 */
public class SketchBinaryFn implements BinaryFn.Identical<Object[]>
{
  private final int nomEntries;
  private final SketchHandler handler;

  public SketchBinaryFn(int nomEntries, SketchHandler handler)
  {
    this.nomEntries = nomEntries;
    this.handler = handler;
  }

  @Override
  public Object[] apply(Object[] value1, Object[] value2)
  {
    if (value1 == null) {
      return value2;
    }
    if (value2 == null) {
      return value1;
    }
    Preconditions.checkArgument(value1.length == value2.length);

    for (int i = 1; i < value1.length; i++) {
      value1[i] = merge((TypedSketch) value1[i], (TypedSketch) value2[i]);
    }
    return value1;
  }

  @SuppressWarnings("unchecked")
  final TypedSketch merge(TypedSketch object1, TypedSketch object2)
  {
    if (object1 == null) {
      return object2;
    }
    if (object2 == null) {
      return object1;
    }
    Preconditions.checkArgument(
        Objects.equals(object1.type(), object2.type()),
        "Type mismatch.. %s with %s", object1.type(), object2.type()
    );
    TypedSketch union = handler.newUnion(nomEntries, object1.type(), null);
    handler.updateWithSketch(union, object1.value());
    handler.updateWithSketch(union, object2.value());
    return handler.toSketch(union);
  }
}
