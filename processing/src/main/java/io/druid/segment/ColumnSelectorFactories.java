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

package io.druid.segment;

import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;

import java.util.List;

/**
 */
public class ColumnSelectorFactories
{
  public static class NotSupports implements ColumnSelectorFactory
  {
    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeFloatColumnSelector");
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeDoubleColumnSelector");
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeLongColumnSelector");
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeObjectColumnSelector");
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      throw new UnsupportedOperationException("getColumnCapabilities");
    }
  }

  public static class Delegated implements ColumnSelectorFactory
  {
    protected final ColumnSelectorFactory delegate;

    public Delegated(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return delegate.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return delegate.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return delegate.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return delegate.makeObjectColumnSelector(columnName);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

  public static abstract class ArrayIndexed implements ColumnSelectorFactory
  {
    protected final ObjectColumnSelector selector;
    protected final Class elementClass;

    protected ArrayIndexed(ObjectColumnSelector selector, Class elementClass)
    {
      this.selector = selector;
      this.elementClass = elementClass;
    }

    public ObjectColumnSelector getSelector()
    {
      return selector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      throw new UnsupportedOperationException("makeMathExpressionSelector");
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      throw new UnsupportedOperationException("getColumnCapabilities");
    }

    protected abstract Object getObject();

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0f;
          }
          if (value instanceof Number) {
            return ((Number) value).floatValue();
          }
          return Float.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0d;
          }
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          Object value = getObject();
          if (value == null) {
            return 0L;
          }
          if (value instanceof Number) {
            return ((Number) value).longValue();
          }
          return Long.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return elementClass;
        }

        @Override
        public Object get()
        {
          return getObject();
        }
      };
    }
  }

  public static final class FixedArrayIndexed extends ArrayIndexed
  {
    private final int index;

    public FixedArrayIndexed(int index, ObjectColumnSelector selector, Class elementClass)
    {
      super(selector, elementClass);
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? value : value.get(index);
    }
  }

  public static final class VariableArrayIndexed extends ArrayIndexed
  {
    private int index = -1;

    public VariableArrayIndexed(ObjectColumnSelector selector, Class elementClass)
    {
      super(selector, elementClass);
    }

    public void setIndex(int index)
    {
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? value : value.get(index);
    }
  }
}