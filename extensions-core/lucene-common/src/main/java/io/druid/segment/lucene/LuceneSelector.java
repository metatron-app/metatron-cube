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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.base.Throwables;
import io.druid.query.filter.DimFilter;
import io.druid.segment.Segment;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.Column;

import java.lang.reflect.Constructor;
import java.util.List;

public abstract class LuceneSelector extends DimFilter.LuceneFilter
{
  protected LuceneSelector(String field, String scoreField) {super(field, scoreField);}

  @Override
  public DimFilter optimize(Segment segment, List<VirtualColumn> virtualColumns)
  {
    Column column = segment == null ? null : Lucenes.findColumnWithLuceneIndex(field, segment.asQueryableIndex(false));
    if (column != null) {
      Class indexClass = column.classOfSecondaryIndex();
      if (Lucenes.class.getClassLoader() != indexClass.getClassLoader()) {
        return swap(this, indexClass.getClassLoader(), params());
      }
    }
    return super.optimize(segment, virtualColumns);
  }

  @SuppressWarnings("unchecked")
  public static <T> T swap(T object, ClassLoader loader, Object... params)
  {
    Class<?> clazz = object.getClass();
    try {
      return (T) loader.loadClass(clazz.getName())
                       .getConstructor(constructor(clazz).getParameterTypes())
                       .newInstance(params);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static Constructor constructor(Class<?> clazz)
  {
    for (Constructor<?> constructor : clazz.getConstructors()) {
      if (constructor.getAnnotation(JsonCreator.class) != null) {
        return constructor;
      }
    }
    throw new UnsupportedOperationException();
  }

  protected abstract Object[] params();
}
