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

package io.druid.data;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.common.KeyBuilder;
import io.druid.common.utils.StringUtils;
import io.druid.java.util.common.IAE;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("envelope")
public class EnvelopeAggregatorFactory extends AggregatorFactory
{
  private static final byte[] CACHE_KEY = new byte[]{0x10};

  private final String name;
  private final String fieldName;

  public EnvelopeAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    if (name == null && fieldName == null) {
      throw new IAE("one of 'name' or 'fieldName' should be set");
    }
    this.name = name == null ? fieldName : name;
    this.fieldName = fieldName == null ? name : fieldName;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (ValueDesc.isGeometry(selector.type())) {
      return new Aggregator.Abstract<Envelope>()
      {
        @Override
        public Envelope aggregate(Envelope envelope)
        {
          final Geometry geometry = GeoToolsUtils.toGeometry(selector.get());
          if (geometry != null) {
            if (envelope == null) {
              envelope = new Envelope();
            }
            envelope.expandToInclude(geometry.getEnvelopeInternal());
          }
          return envelope;
        }

        @Override
        public Object get(Envelope envelope)
        {
          return envelope == null || envelope.isNull() ? null :
                 new double[]{envelope.getMinX(), envelope.getMaxX(), envelope.getMinY(), envelope.getMaxY()};
        }
      };
    }
    return Aggregators.noopAggregator();
  }

  private static final int MIN_X = 0;
  private static final int MAX_X = MIN_X + Double.BYTES;
  private static final int MIN_Y = MAX_X + Double.BYTES;
  private static final int MAX_Y = MIN_Y + Double.BYTES;

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(fieldName);
    if (ValueDesc.isGeometry(selector.type())) {
      return new BufferAggregator.Abstract()
      {
        @Override
        public void init(ByteBuffer buf, int position)
        {
          buf.putDouble(position + MIN_X, 1);
          buf.putDouble(position + MAX_X, 0);
        }

        @Override
        public void aggregate(ByteBuffer buf, int position)
        {
          final Geometry geometry = GeoToolsUtils.toGeometry(selector.get());
          if (geometry == null) {
            return;
          }
          final Envelope envelope = geometry.getEnvelopeInternal();
          final double minX = buf.getDouble(position + MIN_X);
          final double maxX = buf.getDouble(position + MAX_X);
          if (minX > maxX) {
            buf.putDouble(position + MIN_X, envelope.getMinX());
            buf.putDouble(position + MAX_X, envelope.getMaxX());
            buf.putDouble(position + MIN_Y, envelope.getMinY());
            buf.putDouble(position + MAX_Y, envelope.getMaxY());
          } else {
            if (envelope.getMinX() < minX) {
              buf.putDouble(position + MIN_X, envelope.getMinX());
            }
            if (envelope.getMaxX() > maxX) {
              buf.putDouble(position + MAX_X, envelope.getMaxX());
            }
            final double minY = buf.getDouble(position + MIN_Y);
            final double maxY = buf.getDouble(position + MAX_Y);
            if (envelope.getMinY() < minY) {
              buf.putDouble(position + MIN_Y, envelope.getMinY());
            }
            if (envelope.getMaxY() > maxY) {
              buf.putDouble(position + MAX_Y, envelope.getMaxY());
            }
          }
        }

        @Override
        public Object get(ByteBuffer buf, int position)
        {
          final double minX = buf.getDouble(position + MIN_X);
          final double maxX = buf.getDouble(position + MAX_X);
          if (minX > maxX) {
            return null;
          }
          return new double[]{
              minX,
              maxX,
              buf.getDouble(position + MIN_Y),
              buf.getDouble(position + MAX_Y)
          };
        }
      };
    }
    return Aggregators.noopBufferAggregator();
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o1, Object o2)
      {
        // from envelop : minX, minY, maxX, maxY
        final double[] coord1 = (double[]) o1;
        final double[] coord2 = (double[]) o2;
        int compare = Double.compare(coord1[0], coord2[0]);
        if (compare == 0) {
          compare = Double.compare(coord1[2], coord2[2]);
          if (compare == 0) {
            compare = Double.compare(coord1[1], coord2[1]);
            if (compare == 0) {
              return Double.compare(coord1[3], coord2[3]);
            }
          }
        }
        return compare;
      }
    };
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner<List> combiner()
  {
    return new Combiner<List>()
    {
      @Override
      public List combine(List param1, List param2)
      {
        final double minX1 = ((Number) param1.get(0)).doubleValue();
        final double maxX1 = ((Number) param1.get(1)).doubleValue();
        final double minY1 = ((Number) param1.get(2)).doubleValue();
        final double maxY1 = ((Number) param1.get(3)).doubleValue();
        final double minX2 = ((Number) param2.get(0)).doubleValue();
        final double maxX2 = ((Number) param2.get(1)).doubleValue();
        final double minY2 = ((Number) param2.get(2)).doubleValue();
        final double maxY2 = ((Number) param2.get(3)).doubleValue();
        return Arrays.asList(
            minX1 < minX2 ? minX1 : minX2,
            maxX1 > maxX2 ? maxX1 : maxX2,
            minY1 < minY2 ? minY1 : minY2,
            maxY1 > maxY2 ? maxY1 : maxY2
        );
      }
    };
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new EnvelopeAggregatorFactory(name, name)
    {
      @Override
      public Aggregator factorize(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new Aggregator.Simple<double[]>()
        {
          @Override
          public double[] aggregate(double[] envelope)
          {
            final double[] coordinates = ((double[]) selector.get());
            if (envelope == null) {
              envelope = Arrays.copyOf(coordinates, 4);
            } else {
              if (coordinates[0] < envelope[0]) {
                envelope[0] = coordinates[0];
              }
              if (coordinates[1] > envelope[1]) {
                envelope[1] = coordinates[1];
              }
              if (coordinates[2] < envelope[2]) {
                envelope[2] = coordinates[2];
              }
              if (coordinates[3] > envelope[3]) {
                envelope[3] = coordinates[3];
              }
            }
            return envelope;
          }
        };
      }

      @Override
      public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
      {
        final ObjectColumnSelector selector = metricFactory.makeObjectColumnSelector(name);
        return new BufferAggregator.Abstract()
        {
          @Override
          public void init(ByteBuffer buf, int position)
          {
            buf.putDouble(position + MIN_X, 1);
            buf.putDouble(position + MAX_X, 0);
          }

          @Override
          public void aggregate(ByteBuffer buf, int position)
          {
            final double[] coordinates = ((double[]) selector.get());
            final double minX = buf.getDouble(position + MIN_X);
            final double maxX = buf.getDouble(position + MAX_X);
            if (minX > maxX) {
              buf.putDouble(position + MIN_X, coordinates[0]);
              buf.putDouble(position + MAX_X, coordinates[1]);
              buf.putDouble(position + MIN_Y, coordinates[2]);
              buf.putDouble(position + MAX_Y, coordinates[3]);
            } else {
              if (coordinates[0] < minX) {
                buf.putDouble(position + MIN_X, coordinates[0]);
              }
              if (coordinates[1] > maxX) {
                buf.putDouble(position + MAX_X, coordinates[1]);
              }
              final double minY = buf.getDouble(position + MIN_Y);
              final double maxY = buf.getDouble(position + MAX_Y);
              if (coordinates[2] < minY) {
                buf.putDouble(position + MIN_Y, coordinates[2]);
              }
              if (coordinates[3] < minY) {
                buf.putDouble(position + MAX_Y, coordinates[3]);
              }
            }
          }
        };
      }
    };
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object == null || object instanceof long[]) {
      return object;
    }
    if (object instanceof List) {
      final List list = (List) object;
      return new double[]{
          Rows.parseDouble(list.get(0)),
          Rows.parseDouble(list.get(1)),
          Rows.parseDouble(list.get(2)),
          Rows.parseDouble(list.get(3))
      };
    }
    ByteBuffer buffer;
    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      buffer = (ByteBuffer) object;
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
    return new double[]{buffer.getDouble(), buffer.getDouble(), buffer.getDouble(), buffer.getDouble()};
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Arrays.asList(fieldName);
  }

  @Override
  public ValueDesc getOutputType()
  {
    return ValueDesc.DOUBLE_ARRAY;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES * 4;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return builder.append(CACHE_KEY)
                  .append(fieldName);
  }
}
