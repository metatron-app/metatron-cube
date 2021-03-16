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

package io.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.ISE;
import io.druid.query.GeomUtils;
import io.druid.query.GeometryDeserializer;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("geom_collect_point")
public class GeomCollectPointAggregatorFactory extends AggregatorFactory implements AggregatorFactory.SQLSupport
{
  private static final byte[] CACHE_TYPE_ID = new byte[]{0x7F, 0x10, 0x02};

  private final String name;
  private final String columnName;

  @JsonCreator
  public GeomCollectPointAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("columnName") String columnName
  )
  {
    this.name = Preconditions.checkNotNull(name == null ? columnName : name);
    this.columnName = Preconditions.checkNotNull(columnName == null ? name : columnName);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getColumnName()
  {
    return columnName;
  }

  @Override
  public AggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver)
  {
    String columnName = Iterables.getOnlyElement(fieldNames, null);
    if (columnName != null) {
      return new GeomCollectPointAggregatorFactory(name, columnName);
    }
    return null;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(columnName);
    return new Aggregator.Simple()
    {
      private List<Point> collect(Geometry geometry, List<Point> collect)
      {
        if (geometry instanceof Point) {
          collect.add((Point) geometry);
        } else if (geometry instanceof MultiPoint) {
          final int size = geometry.getNumGeometries();
          for (int i = 0; i < size; i++) {
            collect.add((Point) geometry.getGeometryN(i));
          }
        }
        return collect;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Object aggregate(Object current)
      {
        final Geometry geom = GeomUtils.toGeometry(selector.get());
        if (geom != null) {
          return collect(geom, current == null ? Lists.newArrayList() : (List) current);
        }
        return current;
      }

      @Override
      @SuppressWarnings("unchecked")
      public MultiPoint get(Object current)
      {
        List<Point> points = (List) current;
        return new MultiPoint(points.toArray(new Point[0]), GeomUtils.GEOM_FACTORY);
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new Aggregators.RelayBufferAggregator(factorize(metricFactory));
  }

  @Override
  public Comparator getComparator()
  {
    throw new UnsupportedOperationException("getComparator");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Combiner<T> combiner()
  {
    return new Combiner()
    {
      @Override
      public Object combine(Object param1, Object param2)
      {
        MultiPoint geom1 = (MultiPoint) param1;
        MultiPoint geom2 = (MultiPoint) param2;
        if (geom1 == null) {
          return geom2;
        } else if (geom2 == null) {
          return geom1;
        }
        return geom1.union(geom2);
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new GeomCollectPointAggregatorFactory(name, name);
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof Geometry) {
      return object;
    }
    final byte[] buffer;
    if (object instanceof byte[]) {
      buffer = (byte[]) object;
    } else if (object instanceof String) {
      buffer = StringUtils.decodeBase64((String) object);
    } else {
      throw new ISE("?? %s", object.getClass().getSimpleName());
    }
    return GeometryDeserializer.deserialize(buffer);
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(columnName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return GeomUtils.GEOM_TYPE;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return -1;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_TYPE_ID)
                  .append(columnName);
  }

  @Override
  public String toString()
  {
    return "GeomCollectPointAggregatorFactory{" +
           "name='" + name + '\'' +
           ", columnName='" + columnName + '\'' +
           '}';
  }
}
