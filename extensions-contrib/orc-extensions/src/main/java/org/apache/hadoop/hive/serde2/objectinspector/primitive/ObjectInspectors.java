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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import io.druid.data.ValueType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ObjectInspectors
{
  public static ObjectInspector newPrimitiveOI(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return new JavaFloatObjectInspector()
        {
          @Override
          public float get(Object o)
          {
            return ((Number) o).floatValue();
          }
        };
      case LONG:
        return new JavaLongObjectInspector()
        {
          @Override
          public long get(Object o)
          {
            return ((Number) o).longValue();
          }
        };
      case DOUBLE:
        return new JavaDoubleObjectInspector()
        {
          @Override
          public double get(Object o)
          {
            return ((Number) o).doubleValue();
          }
        };
      case STRING:
        return new JavaStringObjectInspector();
      default:
        return null;
    }
  }
}
