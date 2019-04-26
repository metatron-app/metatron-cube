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

package io.druid.hive;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.QueryCacheHelper;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.Aggregators;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ObjectColumnSelector;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ObjectInspectors;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@JsonTypeName("hive.udaf")
public class HiveUDAFAggregatorFactory extends AggregatorFactory.TypeResolving
{
  private static final Logger LOG = new Logger(HiveUDAFAggregatorFactory.class);

  private final String name;
  private final List<String> fieldNames;
  private final String udafName;
  private final List<ValueDesc> inputTypes;
  private final ValueDesc outputType;
  private final ValueDesc finalizedType;
  private final boolean merge;

  private transient GenericUDAFEvaluator evaluator;
  private transient ObjectInspector outputOI;

  public HiveUDAFAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldNames") List<String> fieldNames,
      @JsonProperty("udafName") String udafName,
      @JsonProperty("inputTypes") List<ValueDesc> inputTypes,
      @JsonProperty("outputType") ValueDesc outputType,
      @JsonProperty("finalizedType") ValueDesc finalizedType,
      @JsonProperty("merge") boolean merge
  )
  {
    this.name = name;
    this.fieldNames = fieldNames;
    this.udafName = Preconditions.checkNotNull(udafName, "'udafName' cannot be null");
    this.inputTypes = inputTypes;
    this.outputType = outputType;
    this.finalizedType = finalizedType;
    this.merge = merge;
  }

  public HiveUDAFAggregatorFactory(String name, List<String> fieldNames, String udafName)
  {
    this(name, fieldNames, udafName, null, null, null, false);
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public List<String> getFieldNames()
  {
    return fieldNames;
  }

  @JsonProperty
  public String getUdafName()
  {
    return udafName;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  public HiveUDAFAggregatorFactory withMerge(boolean merge)
  {
    return new HiveUDAFAggregatorFactory(name, fieldNames, udafName, inputTypes, outputType, finalizedType, merge);
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldNames;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ValueDesc> getInputTypes()
  {
    return inputTypes;
  }

  public HiveUDAFAggregatorFactory withInputTypes(List<ValueDesc> inputTypes)
  {
    return new HiveUDAFAggregatorFactory(name, fieldNames, udafName, inputTypes, outputType, finalizedType, merge);
  }

  @Override
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ValueDesc getOutputType()
  {
    return outputType;
  }

  public HiveUDAFAggregatorFactory withOutputType(ValueDesc outputType)
  {
    return new HiveUDAFAggregatorFactory(name, fieldNames, udafName, inputTypes, outputType, finalizedType, merge);
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ValueDesc getFinalizedType()
  {
    return finalizedType;
  }

  public HiveUDAFAggregatorFactory withFinalizedType(ValueDesc finalizedType)
  {
    return new HiveUDAFAggregatorFactory(name, fieldNames, udafName, inputTypes, outputType, finalizedType, merge);
  }

  @Override
  public ValueDesc finalizedType()
  {
    return finalizedType;
  }

  private Combiner<Object> combiner;

  @Override
  public Object finalizeComputation(Object object)
  {
    return getFinalizer().combine(null, object);
  }

  private synchronized Combiner<Object> getFinalizer()
  {
    if (combiner == null) {
      combiner = toCombiner(Mode.FINAL);
    }
    return combiner;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final ObjectColumnSelector[] selectors = new ObjectColumnSelector[fieldNames.size()];
    for (int i = 0; i < selectors.length; i++) {
      selectors[i] = metricFactory.makeObjectColumnSelector(fieldNames.get(i));
    }
    final Object[] params = new Object[selectors.length];

    final GenericUDAFEvaluator evaluator;
    final GenericUDAFEvaluator.AggregationBuffer buffer;
    try {
      evaluator = prepare(toAggregationMode());
      buffer = evaluator.getNewAggregationBuffer();
    }
    catch (HiveException e) {
      throw Throwables.propagate(e);
    }

    return new Aggregator.Abstract()
    {
      @Override
      public void reset()
      {
        try {
          evaluator.reset(buffer);
        }
        catch (HiveException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void aggregate()
      {
        for (int i = 0; i < selectors.length; i++) {
          params[i] = selectors[i].get();
        }
        try {
          synchronized (buffer) {
            evaluator.aggregate(buffer, params);
          }
        }
        catch (HiveException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public Object get()
      {
        try {
          return ObjectInspectors.evaluate(outputOI, evaluator.evaluate(buffer));
        }
        catch (HiveException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private GenericUDAFEvaluator.Mode toAggregationMode()
  {
    return merge ? Mode.PARTIAL2 : Mode.PARTIAL1;
  }

  private GenericUDAFEvaluator prepare(Mode mode) throws SemanticException
  {
    if (evaluator == null) {
      synchronized (this) {
        if (evaluator == null) {
          try {
            final TypeInfo[] typeInfos = new TypeInfo[inputTypes.size()];
            for (int i = 0; i < typeInfos.length; i++) {
              typeInfos[i] = ObjectInspectors.toTypeInfo(inputTypes.get(i));
            }
            final ObjectInspector[] inputOIs;
            if (merge) {
              inputOIs = new ObjectInspector[]{ObjectInspectors.toObjectInspector(outputType)};
            } else {
              inputOIs = new ObjectInspector[inputTypes.size()];
              for (int i = 0; i < inputOIs.length; i++) {
                inputOIs[i] = ObjectInspectors.toObjectInspector(inputTypes.get(i));
              }
            }
            evaluator = getUDAFResolver().getEvaluator(typeInfos);
            outputOI = evaluator.init(mode, inputOIs);
          }
          catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }
    }
    return evaluator;
  }

  private GenericUDAFResolver getUDAFResolver() throws SemanticException
  {
    FunctionInfo functionInfo = Preconditions.checkNotNull(
        HiveFunctions.getFunctionInfo(udafName), "%s is not exists", udafName
    );
    Preconditions.checkArgument(functionInfo.isGenericUDAF(), "%s is not udaf", udafName);
    return functionInfo.getGenericUDAFResolver();
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    return new Aggregators.RelayBufferAggregator()
    {
      @Override
      protected Aggregator newAggregator()
      {
        return factorize(metricFactory);
      }
    };
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.nullFirstNatural();   // cannot know
  }

  @Override
  @SuppressWarnings("unchecked")
  public Combiner combiner()
  {
    return toCombiner(Mode.PARTIAL2);
  }

  private Combiner<Object> toCombiner(Mode mode)
  {
    final HiveUDAFAggregatorFactory factory = withMerge(true);
    try (GenericUDAFEvaluator evaluator = factory.prepare(mode)) {
      final GenericUDAFEvaluator.AggregationBuffer buffer = evaluator.getNewAggregationBuffer();
      return new Combiner<Object>()
      {
        @Override
        public Object combine(Object param1, Object param2)
        {
          try {
            evaluator.reset(buffer);
            evaluator.merge(buffer, param1);
            evaluator.merge(buffer, param2);
            return ObjectInspectors.evaluate(factory.outputOI, evaluator.evaluate(buffer));
          }
          catch (HiveException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public HiveUDAFAggregatorFactory getCombiningFactory()
  {
    return new HiveUDAFAggregatorFactory(name, Arrays.asList(name), udafName, inputTypes, outputType, null, true);
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Ints.BYTES * 2;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNamesBytes = QueryCacheHelper.computeCacheBytes(fieldNames);
    byte[] udafNameBytes = StringUtils.toUtf8WithNullToEmpty(udafName);

    int length = 1 + fieldNamesBytes.length + udafNameBytes.length;
    return ByteBuffer.allocate(length)
                     .put((byte) 0x7e)
                     .put(fieldNamesBytes)
                     .put(udafNameBytes)
                     .array();
  }

  @Override
  public boolean needResolving()
  {
    return inputTypes == null || outputType == null || finalizedType == null;
  }

  @Override
  public AggregatorFactory resolve(Supplier<? extends TypeResolver> supplier)
  {
    HiveUDAFAggregatorFactory factory = this;
    if (factory.inputTypes == null) {
      TypeResolver resolver = supplier.get();
      List<ValueDesc> inputTypes = Lists.newArrayList();
      for (String fieldName : fieldNames) {
        inputTypes.add(Preconditions.checkNotNull(resolver.resolve(fieldName), "failed to resolve [%s]", fieldName));
      }
      factory = factory.withInputTypes(inputTypes);
    }
    if (factory.outputType == null) {
      factory = factory.withOutputType(factory.resolveType(toAggregationMode()));
    }
    if (factory.finalizedType == null) {
      ValueDesc resolved = factory.withMerge(true).resolveType(Mode.FINAL);
      factory = factory.withFinalizedType(resolved);
    }
    return factory;
  }

  private ValueDesc resolveType(Mode mode)
  {
    try (GenericUDAFEvaluator evaluator = prepare(mode)) {
      return Preconditions.checkNotNull(ObjectInspectors.typeOf(outputOI, null), "Cannot resolve output type");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String toString()
  {
    return "HiveUDAFAggregatorFactory{" +
           "name='" + name + '\'' +
           ", udafName='" + udafName + '\'' +
           ", fieldNames=" + fieldNames +
           ", inputTypes=" + inputTypes +
           ", outputType=" + outputType +
           ", finalizedType=" + finalizedType +
           ", merge=" + merge +
           '}';
  }
}
