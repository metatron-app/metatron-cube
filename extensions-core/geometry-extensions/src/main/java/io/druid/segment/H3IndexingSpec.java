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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.DSuppliers;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.UOE;
import io.druid.query.H3Functions;
import io.druid.query.filter.H3PointDistanceFilter;
import io.druid.query.filter.H3Query;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ColumnPartWriter.LongType;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.IOPeon;
import io.druid.segment.filter.BitmapHolder;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.serde.ColumnPartSerde;
import io.druid.segment.serde.ComplexMetrics;
import io.druid.segment.serde.LongGenericColumnPartSerde;
import io.druid.segment.serde.StructMetricSerde;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
@JsonTypeName("h3")
public class H3IndexingSpec implements SecondaryIndexingSpec.WithDescriptor
{
  public static final String INDEX_NAME = "__h3";

  private final String latitude;
  private final String longitude;
  private final int resolution;

  @JsonCreator
  public H3IndexingSpec(
      @JsonProperty("latitude") String latitude,
      @JsonProperty("longitude") String longitude,
      @JsonProperty("resolution") int resolution
  )
  {
    this.latitude = Preconditions.checkNotNull(latitude);
    this.longitude = Preconditions.checkNotNull(longitude);
    this.resolution = resolution;
  }

  @JsonProperty
  public String getLatitude()
  {
    return latitude;
  }

  @JsonProperty
  public String getLongitude()
  {
    return longitude;
  }

  @JsonProperty
  public int getResolution()
  {
    return resolution;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return resolution == ((H3IndexingSpec) o).resolution;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(resolution);
  }

  @Override
  public MetricColumnSerializer serializer(String columnName, final ValueDesc type)
  {
    final int[] ix = extractIx(type, latitude, longitude);

    return new MetricColumnSerializer()
    {
      private LongType writer;

      public void open(IOPeon ioPeon) throws IOException
      {
        writer = LongType.create(ioPeon, String.format("%s.h3", columnName), IndexIO.BYTE_ORDER, CompressionStrategy.LZ4);
        writer.open();
      }

      @Override
      public void serialize(int rowNum, Object input) throws IOException
      {
        long h3 = 0;    // invalid address in H3
        if (input instanceof List) {
          List struct = (List) input;
          double latitude = ((Number) struct.get(ix[0])).doubleValue();
          double longitude = ((Number) struct.get(ix[1])).doubleValue();
          h3 = H3Functions.geoToH3(latitude, longitude, resolution);
        } else if (input instanceof Object[]) {
          Object[] struct = (Object[]) input;
          double latitude = ((Number) struct[ix[0]]).doubleValue();
          double longitude = ((Number) struct[ix[1]]).doubleValue();
          h3 = H3Functions.geoToH3(latitude, longitude, resolution);
        }
        writer.add(h3);
      }

      @Override
      public void close() throws IOException
      {
        writer.close();
      }

      @Override
      public ColumnDescriptor.Builder buildDescriptor(
          IOPeon ioPeon,
          ColumnDescriptor.Builder builder
      )
      {
        return builder.addSerde(new SerDe(writer))
                      .addDescriptor(descriptor(columnName));
      }
    };
  }

  public static final Pattern H3_PATTERN = Pattern.compile("h3\\(latitude=(.+),longitude=(.+),resolution=(\\d+)\\)");

  @Override
  public Map<String, String> descriptor(String column)
  {
    return ImmutableMap.of(
        INDEX_NAME, String.format("h3(latitude=%s,longitude=%s,resolution=%d)", latitude, longitude, resolution)
    );
  }

  @JsonTypeName("h3")
  public static class SerDe implements ColumnPartSerde
  {
    private final LongType writer;

    @JsonCreator
    public SerDe()
    {
      this.writer = null;
    }

    public SerDe(LongType writer)
    {
      this.writer = Preconditions.checkNotNull(writer);
    }

    @Override
    public Serializer getSerializer()
    {
      return writer;
    }

    @Override
    public Deserializer getDeserializer()
    {
      return new Deserializer()
      {
        @Override
        public void read(ByteBuffer buffer, ColumnBuilder builder, BitmapSerdeFactory serdeFactory) throws IOException
        {
          String descriptor = Preconditions.checkNotNull(builder.getColumnDesc(INDEX_NAME), "Missing descriptor");
          Matcher matcher = H3_PATTERN.matcher(descriptor);
          Preconditions.checkArgument(matcher.matches(), "Invalid descriptor %s", descriptor);

          ColumnBuilder dummy = new ColumnBuilder("dummy");
          LongGenericColumnPartSerde serde = new LongGenericColumnPartSerde(IndexIO.BYTE_ORDER, null);
          serde.getDeserializer().read(buffer, dummy, serdeFactory);
          final Column column = dummy.build();

          final int numRows = builder.getNumRows();
          final int resolution = Integer.valueOf(matcher.group(3));

          builder.addSecondaryIndex(
              new ExternalIndexProvider<H3Index>()
              {
                @Override
                public String source()
                {
                  return "h3";
                }

                @Override
                public int numRows()
                {
                  return numRows;
                }

                @Override
                public long getSerializedSize()
                {
                  return column.getSerializedSize(Column.EncodeType.GENERIC);
                }

                @Override
                public Class<? extends H3Index> provides()
                {
                  return H3Index.class;
                }

                @Override
                public H3Index get()
                {
                  return new H3Index()
                  {
                    private final DSuppliers.Memoizing<GenericColumn.LongType> generic =
                        DSuppliers.memoize(() -> (GenericColumn.LongType) column.getGenericColumn());

                    @Override
                    public void close() throws IOException
                    {
                      if (generic.initialized()) {
                        generic.get().close();
                      }
                    }

                    @Override
                    public BitmapHolder filterFor(H3Query query, FilterContext context, String attachment)
                    {
                      BitmapFactory factory = context.bitmapFactory();
                      if (query instanceof H3PointDistanceFilter) {
                        H3PointDistanceFilter pd = (H3PointDistanceFilter) query;
                        Pair<LongSet, LongSet> matched = H3Functions.getMatchH3Ids(
                            pd.getLatitude(), pd.getLongitude(), pd.getRadiusMeters(), resolution);
                        if (matched.lhs.isEmpty() && matched.rhs.isEmpty()) {
                          return BitmapHolder.exact(factory.makeEmptyImmutableBitmap());
                        }
                        if (matched.rhs.isEmpty()) {
                          return BitmapHolder.exact(
                              generic.get().collect(factory, context.rowIterator(), x -> matched.lhs.contains(x))
                          );
                        }
                        final MutableBitmap match = factory.makeEmptyMutableBitmap();
                        final MutableBitmap possible = factory.makeEmptyMutableBitmap();
                        generic.get().scan(
                            context.rowIterator(),
                            (x, v) -> {
                              final long key = v.applyAsLong(x);
                              if (matched.lhs.contains(key)) {
                                match.add(x);
                              } else if (matched.rhs.contains(key)) {
                                match.add(x);
                                possible.add(x);
                              }
                            }
                        );
                        if (possible.isEmpty()) {
                          return BitmapHolder.exact(factory.makeImmutableBitmap(match));
                        }
                        context.attach(query, possible);
                        return BitmapHolder.notExact(factory.makeImmutableBitmap(match));
                      }
                      throw new UOE("?? [%s]", query.getClass().getSimpleName());
                    }
                  };
                }
              }
          );
        }
      };
    }
  }

  public static int[] extractIx(ValueDesc type, String latitude, String longitude)
  {
    Preconditions.checkArgument(type.isStruct(), "only struct type can be used but %s", type);
    StructMetricSerde serde = (StructMetricSerde) Preconditions.checkNotNull(ComplexMetrics.getSerdeForType(type));

    final int indexLat = serde.indexOf(latitude);
    Preconditions.checkArgument(indexLat >= 0, "invalid latitude field %s", latitude);
    Preconditions.checkArgument(
        serde.type(indexLat).isNumeric(), "invalid field type %s for %s", serde.type(indexLat), latitude
    );

    final int indexLon = serde.indexOf(longitude);
    Preconditions.checkArgument(indexLon >= 0, "invalid longitude field %s", longitude);
    Preconditions.checkArgument(
        serde.type(indexLon).isNumeric(), "invalid field type %s for %s", serde.type(indexLon), longitude
    );
    return new int[]{indexLat, indexLon};
  }
}
