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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Function;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 */
@JsonTypeName("knn.vector")
public class KnnVectorStrategy implements LuceneIndexingStrategy
{
  public static final String TYPE_NAME = "knn.vector";

  private final String fieldName;
  private final int maxConn;
  private final int beamWidth;
  private final int dimension;
  private final String distanceMeasure;

  @JsonCreator
  public KnnVectorStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("maxConn") int maxConn,
      @JsonProperty("beamWidth") int beamWidth,
      @JsonProperty("dimension") int dimension,
      @JsonProperty("distanceMeasure") String distanceMeasure
  )
  {
    this.fieldName = fieldName;
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.dimension = dimension;
    this.distanceMeasure = distanceMeasure;
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getMaxConn()
  {
    return maxConn;
  }

  @JsonProperty
  public int getBeamWidth()
  {
    return beamWidth;
  }

  @JsonProperty
  public int getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getDistanceMeasure()
  {
    return distanceMeasure;
  }

  @Override
  public String getFieldDescriptor()
  {
    return StringUtils.isNullOrEmpty(distanceMeasure)
           ? TYPE_NAME
           : String.format("%s(%s)", TYPE_NAME, distanceMeasure.toLowerCase());
  }

  @Override
  public LuceneIndexingStrategy withFieldName(String fieldName)
  {
    return new KnnVectorStrategy(fieldName, maxConn, beamWidth, dimension, distanceMeasure);
  }

  @Override
  public IndexWriterConfig configure(IndexWriterConfig config)
  {
    if (maxConn == 0 && beamWidth == 0) {
      return config;    // 16, 100
    }
    int _maxConn = maxConn == 0 ? Lucene95HnswVectorsFormat.DEFAULT_MAX_CONN : maxConn;
    int _beamWidth = beamWidth == 0 ? Lucene95HnswVectorsFormat.DEFAULT_BEAM_WIDTH : beamWidth;
    return config.setCodec(new Lucene95Codec()
    {
      @Override
      public KnnVectorsFormat getKnnVectorsFormatForField(String field)
      {
        return new Lucene95HnswVectorsFormat(_maxConn, _beamWidth);   // I'm not sure on this
      }
    });
  }

  @Override
  public Function<Object, Field[]> createIndexableField(ValueDesc type)
  {
    VectorSimilarityFunction similarity = convert(distanceMeasure);
    if (type.isArray() && type.unwrapArray(ValueDesc.FLOAT).isPrimitiveNumeric()) {
      if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
        return v -> v == null ? null : new Field[]{new KnnFloatVectorField(fieldName, normalize(toVector(v, dimension)), similarity)};
      }
      return v -> v == null ? null : new Field[]{new KnnFloatVectorField(fieldName, toVector(v, dimension), similarity)};
    }
    throw new IAE("cannot index '%s' as knn.vector", type);
  }

  public static VectorSimilarityFunction distanceMeasure(String descriptor)
  {
    if (TYPE_NAME.equals(descriptor)) {
      return VectorSimilarityFunction.DOT_PRODUCT;
    }
    return convert(descriptor.substring(TYPE_NAME.length() + 1, descriptor.length() - 1));
  }

  private static VectorSimilarityFunction convert(String distanceMeasure)
  {
    if (StringUtils.isNullOrEmpty(distanceMeasure)) {
      return VectorSimilarityFunction.DOT_PRODUCT;
    }
    return VectorSimilarityFunction.valueOf(distanceMeasure.toUpperCase(Locale.ROOT));
  }

  private static float[] toVector(Object x, int dimension)
  {
    if (x instanceof float[]) {
      return Arrays.copyOf((float[]) x, dimension);
    }
    if (x instanceof List) {
      List list = (List) x;
      float[] vector = new float[dimension];
      for (int i = 0; i < vector.length; i++) {
        vector[i] = i < list.size() ? Rows.parseFloat(list.get(i)) : 0f;
      }
      return vector;
    }
    if (x.getClass().isArray()) {
      int length = Array.getLength(x);
      float[] vector = new float[dimension];
      for (int i = 0; i < vector.length; i++) {
        vector[i] = i < length ? Rows.parseFloat(Array.get(x, i)) : 0f;
      }
      return vector;
    }
    throw new IAE("connot convert %s to float vector", x);
  }

  public static float[] normalize(float[] vector)
  {
    final double norm = norm(vector);
    for (int i = 0; i < vector.length; i++) {
      vector[i] /= norm;
    }
    return vector;
  }

  private static double norm(float[] vector)
  {
    double s = 0;
    for (int i = 0; i < vector.length; i++) {
      s += vector[i] * vector[i];
    }
    return Math.sqrt(s);
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

    KnnVectorStrategy that = (KnnVectorStrategy) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }

    return dimension == that.dimension;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, dimension);
  }

  @Override
  public String toString()
  {
    return getFieldDescriptor();
  }
}
