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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class ObjectInspectors
{
  public static ObjectInspector newJavaOI(ValueDesc type)
  {
    if (type.isPrimitive()) {
      return newPrimitiveOI(type.type());
    }
    return null;  // todo
  }

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

  public static ValueDesc typeOf(ObjectInspector inspector, ValueDesc defaultType)
  {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) inspector;
        switch (poi.getPrimitiveCategory()) {
          case BOOLEAN:
          case BYTE:
          case SHORT:
          case INT:
          case LONG:
            return ValueDesc.LONG;
          case FLOAT:
            return ValueDesc.FLOAT;
          case DOUBLE:
            return ValueDesc.DOUBLE;
          case VARCHAR:
          case STRING:
          case CHAR:
            return ValueDesc.STRING;
          case DECIMAL:
            return ValueDesc.DECIMAL;
        }
        return defaultType;
      case LIST:
        return ValueDesc.LIST;
      case MAP:
        return ValueDesc.MAP;
      case STRUCT:
        return ValueDesc.STRUCT;
      default:
        return defaultType;
    }
  }
}
