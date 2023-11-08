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
import com.google.common.base.Preconditions;
import com.google.common.primitives.Floats;
import io.druid.common.utils.StringUtils;
import io.druid.data.Rows;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.VectorUtils;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
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

  private final String similarityFn;
  private final VectorType vectorType;

  @JsonCreator
  public KnnVectorStrategy(
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("maxConn") int maxConn,
      @JsonProperty("beamWidth") int beamWidth,
      @JsonProperty("dimension") int dimension,
      @JsonProperty("similarityFn") String similarityFn,
      @JsonProperty("vectorType") VectorType vectorType
  )
  {
    this.fieldName = fieldName;
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.dimension = dimension;
    this.similarityFn = similarityFn;
    this.vectorType = vectorType == null ? VectorType.FLOATS : vectorType;
    Preconditions.checkArgument(this.vectorType == VectorType.FLOATS || similarityFn == null);
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
  public String getSimilarityFn()
  {
    return similarityFn;
  }

  @JsonProperty
  public VectorType getVectorType()
  {
    return vectorType;
  }

  @Override
  public String getFieldDescriptor()
  {
    return StringUtils.isNullOrEmpty(similarityFn)
           ? TYPE_NAME
           : String.format("%s(%s)", TYPE_NAME, similarityFn.toLowerCase());
  }

  @Override
  public LuceneIndexingStrategy withFieldName(String fieldName)
  {
    return new KnnVectorStrategy(fieldName, maxConn, beamWidth, dimension, similarityFn, vectorType);
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
  public LuceneFieldGenerator createIndexableField(ValueDesc type, Iterable<Object> values)
  {
    Function<Object, Field> generator = createFieldGenerator(type, values, convert(similarityFn));
    return v -> v == null ? null : new Field[] {generator.apply(v)};
  }

  private Function<Object, Field> createFieldGenerator(
      ValueDesc type,
      Iterable<Object> values,
      VectorSimilarityFunction similarity
  )
  {
    if (type.isVector() || (type.isArray() && type.unwrapArray(ValueDesc.FLOAT).isPrimitiveNumeric())) {
      if (vectorType == VectorType.FLOATS) {
        Function<Object, float[]> normalizer = toNormalizer(similarity, dimension);
        return v -> new KnnFloatVectorField(fieldName, normalizer.apply(v), similarity);
      }
      Function<Object, byte[]> quantizer = toQuantizer(vectorType, dimension, values);
      return v -> new KnnByteVectorField(fieldName, quantizer.apply(v));
    }
    throw new IAE("cannot index '%s' as knn.vector", type);
  }

  private static Function<Object, float[]> toNormalizer(VectorSimilarityFunction similarity, int dimension)
  {
    if (similarity == VectorSimilarityFunction.DOT_PRODUCT) {
      return v -> VectorUtils.normalize(toVector(v, dimension));
    }
    return v -> toVector(v, dimension);
  }

  private static Function<Object, byte[]> toQuantizer(VectorType vectorType, int dimension, Iterable<Object> values)
  {
    if (vectorType == VectorType.BYTE_CLIP) {
      return v -> VectorUtils.clip(VectorUtils.normalize(toVector(v, dimension)));
    }
    // todo: how to propagate min/max to description?
    Preconditions.checkArgument(values != null);
    float[] minmax = new float[] {Float.MAX_VALUE, Float.MIN_VALUE};
    for (Object object : values) {
      float[] vector = (float[]) object;
      minmax[0] = Math.min(minmax[0], Floats.min(vector));
      minmax[1] = Math.max(minmax[1], Floats.max(vector));
    }
    return v -> VectorUtils.quantizeGlobal(VectorUtils.normalize(toVector(v, dimension)), minmax);
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
