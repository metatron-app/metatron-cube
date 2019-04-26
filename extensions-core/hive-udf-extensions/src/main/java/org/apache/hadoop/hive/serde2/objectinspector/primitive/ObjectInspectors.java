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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.List;
import java.util.Map;

public class ObjectInspectors
{
  static final JavaFloatObjectInspector FLOAT = new JavaFloatObjectInspector()
  {
    @Override
    public float get(Object o)
    {
      return ((Number) o).floatValue();
    }
  };

  static final JavaDoubleObjectInspector DOUBLE = new JavaDoubleObjectInspector()
  {
    @Override
    public double get(Object o)
    {
      return ((Number) o).doubleValue();
    }
  };

  static final JavaLongObjectInspector LONG = new JavaLongObjectInspector()
  {
    @Override
    public long get(Object o)
    {
      return ((Number) o).longValue();
    }
  };

  static final JavaStringObjectInspector STRING = new JavaStringObjectInspector();

  public static ObjectInspector toObjectInspector(ValueDesc type)
  {
    if (type.isDimension()) {
      return STRING;
    }
    if (type.isPrimitive()) {
      ObjectInspector oi = toPrimitiveOI(type.type());
      if (oi != null) {
        return oi;
      }
    }
    if (type.isList()) {
      String[] descriptions = Preconditions.checkNotNull(type.getDescription());
      Preconditions.checkArgument(descriptions.length == 2);
      return ObjectInspectorFactory.getStandardListObjectInspector(toObjectInspector(ValueDesc.of(descriptions[1])));
    }
    if (type.isStruct()) {
      String[] descriptions = Preconditions.checkNotNull(type.getDescription());
      List<String> names = Lists.newArrayList();
      List<ObjectInspector> ois = Lists.newArrayList();
      for (int i = 1; i < descriptions.length; i++) {
        int index = descriptions[i].indexOf(':');
        Preconditions.checkArgument(index > 0, "invalid description %s", descriptions[i]);
        names.add(descriptions[i].substring(0, index));
        ois.add(Preconditions.checkNotNull(toObjectInspector(ValueDesc.of(descriptions[i].substring(index + 1)))));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(names, ois);
    }
    throw new UnsupportedOperationException("not supported type " + type);
  }

  public static ObjectInspector toPrimitiveOI(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return FLOAT;
      case LONG:
        return LONG;
      case DOUBLE:
        return DOUBLE;
      case STRING:
        return STRING;
      default:
        return null;
    }
  }

  public static TypeInfo toTypeInfo(ValueDesc type)
  {
    if (type.isDimension()) {
      return TypeInfoFactory.stringTypeInfo;
    }
    if (type.isPrimitive()) {
      TypeInfo info = toPrimitiveType(type.type());
      if (info != null) {
        return info;
      }
    }
    if (type.isList()) {
      String[] descriptions = Preconditions.checkNotNull(type.getDescription());
      Preconditions.checkArgument(descriptions.length == 2);
      return TypeInfoFactory.getListTypeInfo(toTypeInfo(ValueDesc.of(descriptions[1])));
    }
    if (type.isStruct()) {
      String[] descriptions = Preconditions.checkNotNull(type.getDescription());
      List<String> names = Lists.newArrayList();
      List<TypeInfo> types = Lists.newArrayList();
      for (int i = 1; i < descriptions.length; i++) {
        int index = descriptions[i].indexOf(':');
        Preconditions.checkArgument(index > 0, "invalid description %s", descriptions[i]);
        names.add(descriptions[i].substring(0, index));
        types.add(Preconditions.checkNotNull(toTypeInfo(ValueDesc.of(descriptions[i].substring(index + 1)))));
      }
      return TypeInfoFactory.getStructTypeInfo(names, types);
    }
    TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(type.typeName());
    if (info != null) {
      return info;
    }
    throw new UnsupportedOperationException("not supported type " + type);
  }

  public static TypeInfo toPrimitiveType(ValueType type)
  {
    switch (type) {
      case FLOAT:
        return TypeInfoFactory.floatTypeInfo;
      case LONG:
        return TypeInfoFactory.longTypeInfo;
      case DOUBLE:
        return TypeInfoFactory.doubleTypeInfo;
      case STRING:
        return TypeInfoFactory.stringTypeInfo;
      default:
        return null;
    }
  }

  private static TypeInfo toTypeInfo(ObjectInspector inspector)
  {
    switch (inspector.getCategory()) {
      case PRIMITIVE:
        return ((PrimitiveObjectInspector) inspector).getTypeInfo();
      case LIST:
        ObjectInspector elementOI = ((ListObjectInspector) inspector).getListElementObjectInspector();
        return TypeInfoFactory.getListTypeInfo(toTypeInfo(elementOI));
      case MAP:
        ObjectInspector keyOI = ((MapObjectInspector) inspector).getMapKeyObjectInspector();
        ObjectInspector valueOI = ((MapObjectInspector) inspector).getMapValueObjectInspector();
        return TypeInfoFactory.getMapTypeInfo(toTypeInfo(keyOI), toTypeInfo(valueOI));
      case STRUCT:
        List<String> names = Lists.newArrayList();
        List<TypeInfo> types = Lists.newArrayList();
        for (StructField field : ((StructObjectInspector) inspector).getAllStructFieldRefs()) {
          names.add(field.getFieldName());
          types.add(toTypeInfo(field.getFieldObjectInspector()));
        }
        return TypeInfoFactory.getStructTypeInfo(names, types);
    }
    return null;
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
        ValueDesc element = typeOf(((ListObjectInspector) inspector).getListElementObjectInspector(), null);
        return element == null ? ValueDesc.LIST : ValueDesc.ofList(element);
      case STRUCT:
        StringBuilder elements = new StringBuilder();
        for (StructField field : ((StructObjectInspector) inspector).getAllStructFieldRefs()) {
          ValueDesc type = Preconditions.checkNotNull(typeOf(field.getFieldObjectInspector(), null));
          if (elements.length() > 0) {
            elements.append(',');
          }
          elements.append(field.getFieldName()).append(':').append(type.typeName());
        }
        return ValueDesc.ofStruct(elements.toString());
      case MAP:
        return ValueDesc.MAP;
      default:
        return defaultType;
    }
  }

  @SuppressWarnings("unchecked")
  public static Object evaluate(ObjectInspector output, Object result)
  {
    Object value = null;
    if (output instanceof PrimitiveObjectInspector) {
      value = ((PrimitiveObjectInspector) output).getPrimitiveJavaObject(result);
    } else if (output instanceof ListObjectInspector) {
      final ListObjectInspector listOI = (ListObjectInspector) output;
      final ObjectInspector elementOI = listOI.getListElementObjectInspector();
      final List list = Lists.newArrayList();
      for (Object element : listOI.getList(result)) {
        list.add(evaluate(elementOI, element));
      }
      value = list;
    } else if (output instanceof MapObjectInspector) {
      final MapObjectInspector mapOI = (MapObjectInspector) output;
      final ObjectInspector keyOI = mapOI.getMapKeyObjectInspector();
      final ObjectInspector valueOI = mapOI.getMapValueObjectInspector();
      final Map map = Maps.newHashMap();
      for (Map.Entry entry : mapOI.getMap(result).entrySet()) {
        map.put(evaluate(keyOI, entry.getKey()), evaluate(valueOI, entry.getValue()));
      }
      value = map;
    } else if (output instanceof StructObjectInspector) {
      final StructObjectInspector structOI = (StructObjectInspector) output;
      final List<? extends StructField> fields = structOI.getAllStructFieldRefs();
      final Object[] array = new Object[fields.size()];
      for (int i = 0; i < array.length; i++) {
        StructField field = fields.get(i);
        array[i] = evaluate(field.getFieldObjectInspector(), structOI.getStructFieldData(result, field));
      }
      value = array;
    }
    return value;
  }
}
