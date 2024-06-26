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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.Cacheable;
import io.druid.common.KeyBuilder;
import io.druid.common.guava.CombineFn;
import io.druid.common.guava.Comparators;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.IOUtils;
import io.druid.data.TypeResolver;
import io.druid.data.TypeUtils;
import io.druid.data.ValueDesc;
import io.druid.data.input.CompactRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.MathExprFilter;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.Metadata;
import io.druid.segment.QueryableIndex;
import io.druid.segment.ScanContext;
import io.druid.segment.Scanning;
import io.druid.segment.Segment;
import io.druid.segment.column.Column;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.GenericColumn.DoubleType;
import io.druid.segment.column.GenericColumn.FloatType;
import io.druid.segment.column.GenericColumn.LongType;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

/**
 * Processing related interface
 * <p>
 * An AggregatorFactory is an object that knows how to generate an Aggregator using a ColumnSelectorFactory.
 * <p>
 * This is useful as an abstraction to allow Aggregator classes to be written in terms of MetricSelector objects
 * without making any assumptions about how they are pulling values out of the base data.  That is, the data is
 * provided to the Aggregator through the MetricSelector object, so whatever creates that object gets to choose how
 * the data is actually stored and accessed.
 */
public abstract class AggregatorFactory implements Cacheable
{
  private static final Logger log = new Logger(AggregatorFactory.class);

  public abstract Aggregator factorize(ColumnSelectorFactory metricFactory);

  public abstract BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory);

  public abstract Comparator getComparator();

  public Comparator getFinalizedComparator()
  {
    return Comparators.toComparator(finalizedType());
  }

  public static interface FinalizingCombinerFactory
  {
    CombineFn.Finalizing build();
  }

  public static CombineFn combiner(AggregatorFactory factory, boolean finalize)
  {
    if (finalize && factory instanceof FinalizingCombinerFactory) {
      return ((FinalizingCombinerFactory) factory).build();
    }
    return wrapNullHandling(factory.combiner());
  }

  @SuppressWarnings("unchecked")
  private static CombineFn.Identical wrapNullHandling(BinaryFn.Identical combiner)
  {
    return (param1, param2) ->
    {
      if (param1 == null) {
        return param2;
      } else if (param2 == null) {
        return param1;
      } else {
        return combiner.apply(param1, param2);
      }
    };
  }

  public AggregatorFactory optimize(Segment segment)
  {
    return this;
  }

  public static List<AggregatorFactory> optimize(List<AggregatorFactory> factories, Segment segment)
  {
    List<AggregatorFactory> list = null;
    for (int i = 0; i < factories.size(); i++) {
      AggregatorFactory origin = factories.get(i);
      AggregatorFactory optimized = origin.optimize(segment);
      if (origin == optimized) {
        continue;
      }
      if (list == null) {
        list = Lists.newArrayList(factories);
      }
      list.set(i, optimized);
    }
    return list == null ? factories : list;
  }

  public static List<AggregatorFactory> optimize(List<AggregatorFactory> factories, Cursor cursor)
  {
    ScanContext context = cursor.scanContext();
    List<AggregatorFactory> list = null;
    for (int i = 0; i < factories.size(); i++) {
      AggregatorFactory origin = factories.get(i);
      AggregatorFactory optimized = context.is(Scanning.FULL) ? origin.optimize(cursor) : origin;
      if (context.awareTargetRows()) {
        optimized = optimized.evaluate(cursor, context);
      }
      if (origin == optimized) {
        continue;
      }
      if (list == null) {
        list = Lists.newArrayList(factories);
      }
      list.set(i, optimized);
    }
    return list == null ? factories : list;
  }

  // called only when Scanning.FULL
  public AggregatorFactory optimize(Cursor cursor)
  {
    return this;
  }

  // called only when Scanning.FULL, Scanning.RANGE, Scanning.BITMAP
  public AggregatorFactory evaluate(Cursor cursor, ScanContext context)
  {
    return this;
  }

  /**
   * A method that knows how to combine the outputs of the getIntermediate() method from the Aggregators
   * produced via factorize().  Note, even though this is called combine, this method's contract *does*
   * allow for mutation of the input objects.  Thus, any use of lhs or rhs after calling this method is
   * highly discouraged.
   * <p>
   * Mostly, it's not dependent to inner state of AggregatorFactory. So I've changed to return function
   *
   * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
   */
  public abstract BinaryFn.Identical combiner();

  /**
   * Returns an AggregatorFactory that can be used to combine the output of aggregators from this factory.  This
   * generally amounts to simply creating a new factory that is the same as the current except with its input
   * column renamed to the same as the output column.
   *
   * @return a new Factory that can be used for operations on top of data output from the current factory.
   */
  public abstract AggregatorFactory getCombiningFactory();

  /**
   * Returns an AggregatorFactory that can be used to merge the output of aggregators from this factory and
   * other factory.
   * This method is relevant only for AggregatorFactory which can be used at ingestion time.
   *
   * @return a new Factory that can be used for merging the output of aggregators from this factory and other.
   */
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (isMergeable(other)) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  protected boolean isMergeable(AggregatorFactory other)
  {
    return getName().equals(other.getName()) && getClass() == other.getClass();
  }

  /**
   * A method that knows how to "deserialize" the object from whatever form it might have been put into
   * in order to transfer via JSON.
   *
   * @param object the object to deserialize
   *
   * @return the deserialized object
   */
  public Object deserialize(Object object)
  {
    return object;
  }

  public abstract String getName();

  public abstract List<String> requiredFields();

  public abstract KeyBuilder getCacheKey(KeyBuilder builder);

  public List<String> getExtractHints()
  {
    return Arrays.asList();
  }

  // this is type for ingestion, which can be different from typeName (which is output type from serde)
  public ValueDesc getInputType()
  {
    return getOutputType();
  }

  public ValueDesc getOutputType(boolean finalized)
  {
    return finalized ? finalizedType() : getOutputType();
  }

  public abstract ValueDesc getOutputType();

  /**
   * "Finalizes" the computation of an object.  Primarily useful for complex types that have a different mergeable
   * intermediate format than their final resultant output.
   *
   * @param object the object to be finalized
   *
   * @return the finalized value that should be returned for the initial query
   */
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  public ValueDesc finalizedType()
  {
    return getOutputType();
  }

  /**
   * Returns the maximum size that this aggregator will require in bytes for intermediate storage of results.
   *
   * @return the maximum number of bytes that an aggregator of this type will require for intermediate result storage.
   */
  public abstract int getMaxIntermediateSize();

  /**
   * This AggregatorFactory returns EstimableAggregator which provides closer estimation of memory usage in ingestion
   *
   * @return
   */
  public boolean providesEstimation()
  {
    return false;
  }

  public static interface CubeSupport
  {
    String getCubeName();

    String getPredicate();

    String getFieldName();

    AggregatorFactory getCombiningFactory(String inputField);
  }

  // this is possible only when intermediate type conveys resolved-type back to broker
  public static interface TypeResolving
  {
    boolean needResolving();

    AggregatorFactory resolve(Supplier<? extends TypeResolver> resolver);
  }

  public AggregatorFactory resolveIfNeeded(Supplier<? extends TypeResolver> resolver)
  {
    if (this instanceof TypeResolving) {
      TypeResolving resolving = (TypeResolving) this;
      if (resolving.needResolving()) {
        return resolving.resolve(resolver);
      }
    }
    return this;
  }

  public static interface Vectorizable
  {
    boolean supports(QueryableIndex index);

    Aggregator.Vectorized create(ColumnSelectorFactory factory);
  }

  /**
   * Merges the list of AggregatorFactory[] (presumable from metadata of some segments being merged) and
   * returns merged AggregatorFactory[] (for the metadata for merged segment).
   * Null is returned if it is not possible to do the merging for any of the following reason.
   * - one of the element in input list is null i.e. aggregators for one the segments being merged is unknown
   * - AggregatorFactory of same name can not be merged if they are not compatible
   *
   * @param aggregatorsList
   *
   * @return merged AggregatorFactory[] or Null if merging is not possible.
   */
  public static AggregatorFactory[] mergeAggregators(List<AggregatorFactory[]> aggregatorsList)
  {
    if (aggregatorsList == null || aggregatorsList.isEmpty()) {
      return null;
    }
    for (AggregatorFactory[] aggregators : aggregatorsList) {
      if (aggregators == null) {
        return null;
      }
    }

    final Map<String, AggregatorFactory> mergedAggregators = new LinkedHashMap<>();
    try {
      for (AggregatorFactory[] aggregators : aggregatorsList) {
        for (AggregatorFactory aggregator : aggregators) {
          String name = aggregator.getName();
          AggregatorFactory other = mergedAggregators.get(name);
          if (other != null) {
            mergedAggregators.put(name, other.getMergingFactory(aggregator));
          } else {
            mergedAggregators.put(name, aggregator);
          }
        }
      }
    }
    catch (AggregatorFactoryNotMergeableException ex) {
      log.warn(ex, "failed to merge aggregator factories");
      return null;
    }

    return mergedAggregators.values().toArray(new AggregatorFactory[0]);
  }

  public static Map<String, AggregatorFactory> asMap(AggregatorFactory[] aggregators)
  {
    return asMap(Arrays.asList(aggregators));
  }

  public static Map<String, AggregatorFactory> asMap(List<AggregatorFactory> aggregators)
  {
    Map<String, AggregatorFactory> map = Maps.newLinkedHashMap();
    if (aggregators != null) {
      for (AggregatorFactory factory : aggregators) {
        map.put(factory.getName(), factory);
      }
    }
    return map;
  }

  public static List<String> toNames(List<AggregatorFactory> aggregators)
  {
    return GuavaUtils.transform(aggregators, AggregatorFactory::getName);
  }

  public static List<String> toNames(List<AggregatorFactory> aggregators, List<PostAggregator> postAggregators)
  {
    return Lists.newArrayList(Sets.newLinkedHashSet(
        Iterables.concat(toNames(aggregators), PostAggregators.toNames(postAggregators)))
    );
  }

  public static String[] toNamesAsArray(List<AggregatorFactory> aggregators)
  {
    return toNames(aggregators).toArray(new String[aggregators.size()]);
  }

  public static Set<String> getRequiredFields(List<AggregatorFactory> aggregators)
  {
    Set<String> required = Sets.newHashSet();
    aggregators.forEach(f -> required.addAll(f.requiredFields()));
    return required;
  }

  public static Map<String, ValueDesc> toExpectedInputType(AggregatorFactory[] aggregators)
  {
    Map<String, ValueDesc> types = Maps.newHashMap();
    for (AggregatorFactory factory : aggregators) {
      Set<String> required = Sets.newHashSet(factory.requiredFields());
      if (required.size() == 1) {
        String column = Iterables.getOnlyElement(required);
        ValueDesc type = factory.getInputType();
        ValueDesc prev = types.put(column, type);
        if (prev != null && !prev.equals(type)) {
          // conflicting..
          types.put(column, ValueDesc.toCommonType(prev, type));
          log.warn("Type conflict on %s (%s and %s)", column, prev, type);
        }
      }
    }
    return types;
  }

  public static Map<String, ValueDesc> toFinalizedType(Map<String, AggregatorFactory> aggregators)
  {
    Map<String, ValueDesc> types = Maps.newHashMap();
    for (Map.Entry<String, AggregatorFactory> entry : aggregators.entrySet()) {
      types.put(entry.getKey(), entry.getValue().finalizedType());
    }
    return types;
  }

  public static List<AggregatorFactory> retain(List<AggregatorFactory> aggregators, List<String> retainer)
  {
    Set<String> set = Sets.newHashSet(retainer);
    List<AggregatorFactory> retained = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      if (set.contains(aggregator.getName())) {
        retained.add(aggregator);
      }
    }
    return retained;
  }

  public static Map<String, AggregatorFactory> getAggregatorsFromMeta(Metadata metadata)
  {
    if (metadata != null && metadata.getAggregators() != null) {
      return asMap(metadata.getAggregators());
    }
    return null;
  }

  public static AggregatorFactory[] toCombinerFactory(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiners = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < combiners.length; i++) {
      combiners[i] = aggregators[i].getCombiningFactory();
    }
    return combiners;
  }

  public static List<AggregatorFactory> toCombinerFactory(Iterable<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory> combiners = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      combiners.add(aggregator.getCombiningFactory());
    }
    return combiners;
  }

  public static BinaryFn.Identical[] toCombiner(AggregatorFactory[] aggregators)
  {
    BinaryFn.Identical[] combiners = new BinaryFn.Identical[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiners[i] = AggregatorFactory.combiner(aggregators[i], false);
    }
    return combiners;
  }

  public static CombineFn[] toCombiner(Iterable<AggregatorFactory> aggregators)
  {
    return toCombiner(aggregators, false);
  }

  public static CombineFn[] toCombiner(Iterable<AggregatorFactory> aggregators, boolean finalize)
  {
    List<CombineFn> combiners = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      combiners.add(AggregatorFactory.combiner(aggregator, finalize));
    }
    return combiners.toArray(new CombineFn[0]);
  }

  public static List<AggregatorFactory> toRelay(Iterable<Pair<String, ValueDesc>> metricAndTypes)
  {
    List<AggregatorFactory> relay = Lists.newArrayList();
    for (Pair<String, ValueDesc> pair : metricAndTypes) {
      relay.add(new RelayAggregatorFactory(pair.lhs, pair.rhs));
    }
    return relay;
  }

  public static int bufferNeeded(List<AggregatorFactory> factories)
  {
    return Math.max(1, factories.stream().mapToInt(f -> f.getMaxIntermediateSize()).sum());
  }

  public static boolean isCountAll(AggregatorFactory factory)
  {
    if (factory instanceof CountAggregatorFactory) {
      CountAggregatorFactory count = (CountAggregatorFactory) factory;
      return count.getFieldName() == null && count.getPredicate() == null;
    }
    return false;
  }

  public static interface SQLSupport
  {
    AggregatorFactory rewrite(String name, List<String> fieldNames, TypeResolver resolver);
  }

  public static SQLBundle bundleSQL(AggregatorFactory.SQLSupport instance)
  {
    return bundleSQL(instance, null);
  }

  public static SQLBundle bundleSQL(AggregatorFactory.SQLSupport instance, PostAggregator.SQLSupport postAggregator)
  {
    JsonTypeName typeName = Preconditions.checkNotNull(
        instance.getClass().getAnnotation(JsonTypeName.class), "missing @JsonTypeName in %s", instance.getClass()
    );
    return new SQLBundle(typeName.value(), instance, postAggregator);
  }

  public static class SQLBundle
  {
    public final String opName;
    public final AggregatorFactory.SQLSupport aggregator;
    public final PostAggregator.SQLSupport postAggregator;

    public SQLBundle(String opName, AggregatorFactory.SQLSupport aggregator)
    {
      this(opName, aggregator, null);
    }

    public SQLBundle(String opName, AggregatorFactory.SQLSupport aggregator, PostAggregator.SQLSupport postAggregator)
    {
      this.opName = Preconditions.checkNotNull(opName);
      this.aggregator = Preconditions.checkNotNull(aggregator);
      this.postAggregator = postAggregator;
    }
  }

  public static interface PredicateSupport
  {
    String getPredicate();

    AggregatorFactory withPredicate(String predicate);

    default AggregatorFactory andPredicate(String predicate)
    {
      if (getPredicate() != null) {
        predicate = String.format("(%s) AND (%s)", getPredicate(), predicate);
      }
      return withPredicate(predicate);
    }
  }

  public static AggregatorFactory wrap(AggregatorFactory factory, DimFilter predicate)
  {
    if (predicate == null) {
      return factory;
    }
    if (factory instanceof PredicateSupport &&
        predicate instanceof MathExprFilter && ((MathExprFilter) predicate).getExpression() == null) {
      return ((PredicateSupport) factory).andPredicate(((MathExprFilter) predicate).getExpression());
    }
    return FilteredAggregatorFactory.of(factory, predicate);
  }

  public static AggregatorFactory constant(AggregatorFactory source, Object value)
  {
    return new ConstantFactory(source, value);
  }

  public static class ConstantFactory extends DelegatedAggregatorFactory
  {
    private final Object value;

    public ConstantFactory(AggregatorFactory delegate, Object value)
    {
      super(delegate);
      this.value = value;
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory factory) {return Aggregator.relay(value);}

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory) {return BufferAggregator.relay(value);}

    @Override
    public int getMaxIntermediateSize() {return 0;}
  }

  public static Row constansToRow(long timestamp, List<AggregatorFactory> constants, boolean compact)
  {
    if (compact) {
      Object[] values = new Object[constants.size() + 1];
      for (int i = 0; i < constants.size(); i++) {
        values[i + 1] = ((ConstantFactory) constants.get(i)).value;
      }
      values[0] = new MutableLong(timestamp);
      return new CompactRow(values);
    }
    Map<String, Object> values = Maps.newHashMapWithExpectedSize(constants.size());
    for (AggregatorFactory factory : constants) {
      values.put(factory.getName(), ((ConstantFactory) factory).value);
    }
    return new MapBasedRow(timestamp, values);
  }

  public static boolean isAllConstant(List<AggregatorFactory> factories)
  {
    for (AggregatorFactory factory : factories) {
      if (!(factory instanceof ConstantFactory)) {
        return false;
      }
    }
    return true;
  }

  public static interface FieldExpressionSupport
  {
    String getFieldName();

    String getFieldExpression();

    AggregatorFactory withFieldExpression(String fieldExpression);
  }

  public static abstract class NumericEvalSupport extends AggregatorFactory
  {
    protected abstract Pair<String, Object> evaluateOn();    // column, null-value pair

    @Override
    public AggregatorFactory evaluate(Cursor cursor, ScanContext context)
    {
      Pair<String, Object> target = evaluateOn();
      if (target != null) {
        Column column = cursor.getColumn(target.lhs);
        if (column == null) {
          return this;  // todo: handle virual column
        }
        if (!column.hasGenericColumn()) {
          return AggregatorFactory.constant(this, target.rhs);
        }
        GenericColumn generic = column.getGenericColumn();
        try {
          if (generic instanceof LongType) {
            return AggregatorFactory.constant(this, evaluate(((LongType) generic).stream(context.iterator())));
          } else if (generic instanceof FloatType) {
            return AggregatorFactory.constant(this, evaluate(((FloatType) generic).stream(context.iterator())));
          } else if (generic instanceof DoubleType) {
            return AggregatorFactory.constant(this, evaluate(((DoubleType) generic).stream(context.iterator())));
          }
        }
        finally {
          IOUtils.closeQuietly(generic);
        }
      }
      return this;
    }

    protected abstract Object evaluate(LongStream stream);

    protected abstract Object evaluate(DoubleStream stream);
  }

  public static PostAggregator asFinalizer(final String outputName, final AggregatorFactory factory)
  {
    return new FinalizingPostAggregator(outputName, factory);
  }

  private static class FinalizingPostAggregator extends PostAggregator.Abstract
  {
    private final String outputName;
    private final AggregatorFactory factory;

    private FinalizingPostAggregator(String outputName, AggregatorFactory factory)
    {
      this.outputName = outputName;
      this.factory = factory;
    }

    @Override
    public String getName()
    {
      return outputName;
    }

    @Override
    public Set<String> getDependentFields()
    {
      return ImmutableSet.of(factory.getName());
    }

    @Override
    public Comparator getComparator()
    {
      return factory.getFinalizedComparator();
    }

    @Override
    public Processor processor(TypeResolver resolver)
    {
      return new AbstractProcessor()
      {
        @Override
        public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
        {
          return factory.finalizeComputation(combinedAggregators.get(factory.getName()));
        }
      };
    }

    @Override
    public ValueDesc resolve(TypeResolver bindings)
    {
      return factory.finalizedType();
    }

    @Override
    public boolean equals(Object other)
    {
      return outputName.equals(((FinalizingPostAggregator) other).outputName) &&
             factory.equals(((FinalizingPostAggregator) other).factory);
    }

    @Override
    public String toString()
    {
      return "FinalizingPostAggregator{" +
             "name='" + factory.getName() + '\'' +
             ", outputName='" + outputName + '\'' +
             '}';
    }
  }

  public static AggregatorFactory[] parse(JsonNode node)
  {
    if (node.isArray()) {
      List<AggregatorFactory> factories = Lists.newArrayList();
      for (JsonNode element : node) {
        Preconditions.checkArgument(element.isObject());
        String name = Preconditions.checkNotNull(element.get("name")).asText();
        JsonNode typeName = element.get("typeName");
        if (typeName != null) {
          factories.add(RelayAggregatorFactory.of(name, ValueDesc.of(typeName.asText())));
          continue;
        }
        JsonNode typeExpr = element.get("typeExpr");
        if (typeExpr != null) {
          String parsed = TypeUtils.parseType(typeExpr);
          if (parsed == null) {
            throw new IAE("cannot parse node %s", parsed);
          }
          factories.add(RelayAggregatorFactory.of(name, ValueDesc.of(parsed)));
          continue;
        }
        throw new IAE("cannot parse node %s", element);
      }
      return factories.toArray(new AggregatorFactory[0]);
    } else if (node.isObject()) {
      List<AggregatorFactory> factories = Lists.newArrayList();
      Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
      while (fields.hasNext()) {
        Map.Entry<String, JsonNode> entry = fields.next();
        String parsed = TypeUtils.parseType(entry.getValue());
        if (parsed == null) {
          throw new IAE("cannot parse node %s", parsed);
        }
        factories.add(RelayAggregatorFactory.of(entry.getKey(), ValueDesc.of(parsed)));
      }
      return factories.toArray(new AggregatorFactory[0]);
    } else if (node.isNull()) {
      return null;
    }
    throw new IAE("cannot parse node %s", node);
  }

  public static class LiteralAggregatorFactory extends AggregatorFactory
  {
    private final String name;
    private final ValueDesc type;
    private final Object value;

    public LiteralAggregatorFactory(
        @JsonProperty("name") String name,
        @JsonProperty("valuleType") ValueDesc valuleType,
        @JsonProperty("value") Object value
    )
    {
      this.name = name;
      this.type = valuleType;
      this.value = valuleType.cast(value);
    }

    @Override
    public Aggregator factorize(ColumnSelectorFactory factory)
    {
      return Aggregator.relay(value);
    }

    @Override
    public BufferAggregator factorizeBuffered(ColumnSelectorFactory factory)
    {
      return BufferAggregator.relay(value);
    }

    @Override
    public int getMaxIntermediateSize()
    {
      return 0;
    }

    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder.disable();
    }

    @Override
    public Comparator getComparator()
    {
      return (x, y) -> 0;
    }

    @Override
    public BinaryFn.Identical combiner()
    {
      return (x, y) -> x;
    }

    @Override
    public AggregatorFactory getCombiningFactory()
    {
      return this;
    }

    @Override
    @JsonProperty
    public String getName()
    {
      return name;
    }

    @Override
    @JsonProperty("valuleType")
    public ValueDesc getOutputType()
    {
      return type;
    }

    @JsonProperty
    public Object getValue()
    {
      return value;
    }

    @Override
    public List<String> requiredFields()
    {
      return Arrays.asList();
    }

    @Override
    public boolean equals(Object other)
    {
      if (!(other instanceof LiteralAggregatorFactory)) {
        return false;
      }
      LiteralAggregatorFactory f = (LiteralAggregatorFactory) other;
      return name.equals(f.name) && type.equals(f.type) && Objects.equals(value, f.value);
    }

    @Override
    public String toString()
    {
      return "LiteralAggregatorFactory{value=" + value + "}";
    }
  }
}
