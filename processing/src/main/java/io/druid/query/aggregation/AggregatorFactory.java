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

package io.druid.query.aggregation;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import io.druid.common.Cacheable;
import io.druid.data.ValueDesc;
import io.druid.query.RowResolver;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.Metadata;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  /**
   * A method that knows how to combine the outputs of the getIntermediate() method from the Aggregators
   * produced via factorize().  Note, even though this is called combine, this method's contract *does*
   * allow for mutation of the input objects.  Thus, any use of lhs or rhs after calling this method is
   * highly discouraged.
   *
   * Mostly, it's not dependent to inner state of AggregatorFactory. So I've changed to return function
   *
   * @param lhs The left hand side of the combine
   * @param rhs The right hand side of the combine
   *
   * @return an object representing the combination of lhs and rhs, this can be a new object or a mutation of the inputs
   */
  public abstract <T> Combiner<T> combiner();

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
    throw new UnsupportedOperationException(String.format(
        "[%s] does not implement getMergingFactory(..)",
        this.getClass().getName()
    ));
  }

  @SuppressWarnings("unchecked")
  protected <T extends AggregatorFactory> T checkMergeable(AggregatorFactory other)
      throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return (T) other;
    }
    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  /**
   * A method that knows how to "deserialize" the object from whatever form it might have been put into
   * in order to transfer via JSON.
   *
   * @param object the object to deserialize
   *
   * @return the deserialized object
   */
  public abstract Object deserialize(Object object);

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

  public abstract String getName();

  public abstract List<String> requiredFields();

  public abstract ValueDesc getOutputType();

  // this is type for ingestion, which can be different from typeName (which is output type from serde)
  public ValueDesc getInputType()
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
   * This AggregatorFactory returns EstimableAggregator which provides more closer estimation of memory usage in ingestion
   *
   * @return
   */
  public boolean providesEstimation()
  {
    return false;
  }

  // this is possible only when intermediate type conveys resolved-type back to broker
  public static abstract class TypeResolving extends AggregatorFactory
  {
    public abstract boolean needResolving();

    public abstract AggregatorFactory resolve(Supplier<RowResolver> resolver);
  }

  public AggregatorFactory resolveIfNeeded(Supplier<RowResolver> resolver)
  {
    if (this instanceof TypeResolving) {
      TypeResolving resolving = (TypeResolving) this;
      if (resolving.needResolving()) {
        return resolving.resolve(resolver);
      }
    }
    return this;
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
    return Lists.newArrayList(
        Lists.transform(
            aggregators, new Function<AggregatorFactory, String>()
            {
              @Override
              public String apply(AggregatorFactory input)
              {
                return input.getName();
              }
            }
        )
    );
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

  public static Map<String, AggregatorFactory> getAggregatorsFromMeta(Metadata metadata)
  {
    if (metadata != null && metadata.getAggregators() != null) {
      return asMap(metadata.getAggregators());
    }
    return Maps.newHashMap();
  }

  public static AggregatorFactory[] toCombinerFactory(AggregatorFactory[] aggregators)
  {
    AggregatorFactory[] combiners = new AggregatorFactory[aggregators.length];
    for (int i = 0; i < combiners.length; i++) {
      combiners[i] = aggregators[i].getCombiningFactory();
    }
    return combiners;
  }

  public static List<AggregatorFactory> toCombinerFactory(List<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory> combiners = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      combiners.add(aggregator.getCombiningFactory());
    }
    return combiners;
  }

  public static AggregatorFactory.Combiner[] toCombiner(AggregatorFactory[] aggregators)
  {
    AggregatorFactory.Combiner[] combiners = new AggregatorFactory.Combiner[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      combiners[i] = aggregators[i].combiner();
    }
    return combiners;
  }

  public static List<AggregatorFactory.Combiner> toCombiner(List<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory.Combiner> combiners = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      combiners.add(aggregator.combiner());
    }
    return combiners;
  }

  public static AggregatorFactory.Combiner[] toCombinerArray(List<AggregatorFactory> aggregators)
  {
    return toCombiner(aggregators).toArray(new Combiner[0]);
  }

  public static List<AggregatorFactory> toRelay(List<AggregatorFactory> aggregators)
  {
    List<AggregatorFactory> relay = Lists.newArrayList();
    for (AggregatorFactory aggregator : aggregators) {
      AggregatorFactory combiner = aggregator.getCombiningFactory();
      relay.add(new RelayAggregatorFactory(combiner.getName(), combiner.getOutputType()));
    }
    return relay;
  }

  public static List<AggregatorFactory> toRelay(List<String> names, List<ValueDesc> types)
  {
    List<AggregatorFactory> relay = Lists.newArrayList();
    for (int i = 0; i < names.size(); i++) {
      relay.add(new RelayAggregatorFactory(names.get(i), types.get(i)));
    }
    return relay;
  }

  public static Function<AggregatorFactory, Pair<String, ValueDesc>> NAME_TYPE =
      new Function<AggregatorFactory, Pair<String, ValueDesc>>()
      {
        @Override
        public Pair<String, ValueDesc> apply(AggregatorFactory input)
        {
          return Pair.of(input.getName(), input.getOutputType());
        }
      };

  public static interface Combiner<T>
  {
    T combine(T param1, T param2);
  }
}
