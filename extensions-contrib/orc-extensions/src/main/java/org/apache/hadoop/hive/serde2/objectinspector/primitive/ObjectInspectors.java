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
