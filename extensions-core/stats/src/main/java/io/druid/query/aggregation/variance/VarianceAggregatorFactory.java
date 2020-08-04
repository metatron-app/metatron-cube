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

package io.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Iterables;
import io.druid.common.KeyBuilder;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.GenericAggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 */
@JsonTypeName("variance")
public class VarianceAggregatorFactory extends GenericAggregatorFactory implements AggregatorFactory.SQLSupport
{
  protected static final byte CACHE_TYPE_ID = 16;

  protected final String estimator;
  protected final boolean isVariancePop;

  @JsonCreator
  public VarianceAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("fieldExpression") String fieldExpression,
      @JsonProperty("predicate") String predicate,
      @JsonProperty("estimator") String estimator,
      @JsonProperty("inputType") ValueDesc inputType
  )
  {
    super(name, fieldName, fieldExpression, predicate, inputType);
    this.estimator = estimator;
    this.isVariancePop = VarianceAggregatorCollector.isVariancePop(estimator);
  }

  public VarianceAggregatorFactory(String name, String fieldName, String estimator, ValueDesc inputType)
  {
    this(name, fieldName, null, null, estimator, inputType);
  }

  public VarianceAggregatorFactory(String name, String fieldName, ValueDesc inputType)
  {
    this(name, fieldName, null, null, null, inputType);
  }

  public VarianceAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, ValueDesc.DOUBLE);
  }

  @Override
  protected ValueDesc toOutputType(ValueDesc inputType)
  {
    ValueDesc variance = ValueDesc.of("variance", VarianceAggregatorCollector.class);
    return ValueDesc.isArray(inputType) ? ValueDesc.elementOfArray(variance) : variance;
  }

  @Override
  public String getCubeName()
  {
    return "variance";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return VarianceAggregatorCollector.getMaxIntermediateSize();
  }

  @Override
  protected Aggregator factorize(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return VarianceAggregator.create(
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return VarianceAggregator.create(
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return VarianceAggregator.create(
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        switch (valueType.typeName()) {
          case "variance":
          case "varianceCombined":
            return VarianceAggregator.create(
                ColumnSelectors.getObjectColumnSelector(
                    metricFactory,
                    fieldName,
                    fieldExpression
                ),
                ColumnSelectors.toMatcher(predicate, metricFactory)
            );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  protected BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory, ValueDesc valueType)
  {
    switch (valueType.type()) {
      case FLOAT:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getFloatColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case DOUBLE:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getDoubleColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case LONG:
        return VarianceBufferAggregator.create(
            name,
            ColumnSelectors.getLongColumnSelector(
                metricFactory,
                fieldName,
                fieldExpression
            ),
            ColumnSelectors.toMatcher(predicate, metricFactory)
        );
      case COMPLEX:
        switch (valueType.typeName()) {
          case "variance":
          case "varianceCombined":
            return VarianceBufferAggregator.create(
                name,
                ColumnSelectors.getObjectColumnSelector(
                    metricFactory,
                    fieldName,
                    fieldExpression
                ),
                ColumnSelectors.toMatcher(predicate, metricFactory)
            );
        }
    }
    throw new IAE(
        "Incompatible type for metric[%s], expected a float, double, long or variance, got a %s", fieldName, inputType
    );
  }

  @Override
  protected AggregatorFactory withName(String name, String fieldName, ValueDesc inputType)
  {
    return new VarianceAggregatorFactory(name, fieldName, fieldExpression, predicate, estimator, inputType);
  }

  @Override
  protected AggregatorFactory withExpression(String name, String fieldExpression, ValueDesc inputType)
  {
    return new VarianceAggregatorFactory(name, fieldName, fieldExpression, predicate, estimator, inputType);
  }

  @Override
  protected byte cacheTypeID()
  {
    return CACHE_TYPE_ID;
  }

  @Override
  public Comparator getComparator()
  {
    return VarianceAggregatorCollector.COMPARATOR;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return new Combiner<Object>()
    {
      @Override
      public Object combine(Object param1, Object param2)
      {
        return VarianceAggregatorCollector.combineValues(param1, param2);
      }
    };
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object == null ? null : ((VarianceAggregatorCollector) object).getVariance(isVariancePop);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public Object deserialize(Object object)
  {
    if (object instanceof byte[]) {
      return VarianceAggregatorCollector.from(ByteBuffer.wrap((byte[]) object));
    } else if (object instanceof ByteBuffer) {
      return VarianceAggregatorCollector.from((ByteBuffer) object);
    } else if (object instanceof String) {
      return VarianceAggregatorCollector.from(
          ByteBuffer.wrap(StringUtils.decodeBase64((String) object))
      );
    }
    return object;
  }

  @JsonProperty
  public String getEstimator()
  {
    return estimator;
  }

  @Override
  public KeyBuilder getCacheKey(KeyBuilder builder)
  {
    return super.getCacheKey(builder)
                .append(isVariancePop);
  }

  @Override
  public VarianceAggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver)
  {
    if (fieldNames.size() != 1) {
      return null;
    }
    String fieldName = Iterables.getOnlyElement(fieldNames);
    ValueDesc inputType = resolver.resolve(fieldName);
    return new VarianceAggregatorFactory(name, fieldName, null, predicate, estimator, inputType);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + '{' +
           "name='" + name + '\'' +
           (fieldName == null ? "" : ", fieldName='" + fieldName + '\'') +
           (fieldExpression == null ? "" : ", fieldExpression='" + fieldExpression + '\'') +
           (predicate == null ? "" : ", predicate='" + predicate + '\'') +
           ", estimator='" + estimator + '\'' +
           ", inputType='" + inputType + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (!super.equals(o)) {
      return false;
    }
    VarianceAggregatorFactory that = (VarianceAggregatorFactory) o;
    return Objects.equals(isVariancePop, that.isVariancePop);
  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + Objects.hashCode(isVariancePop);
    return result;
  }
}
