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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.MergeSequence;
import io.druid.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.UOE;
import io.druid.js.JavaScriptConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.JavaScriptAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.MetricManipulatorFns;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryRunnerTestHelper extends TestHelper
{
  public static final DataSegment descriptor = DataSegment.asKey("testSegment");
  public static final String dataSource = "testing";
  public static final UnionDataSource unionDataSource = UnionDataSource.of(
      Lists.newArrayList(dataSource, dataSource, dataSource, dataSource)
  );

  public static final DateTime minTime = new DateTime("2011-01-12T00:00:00.000Z");

  public static final String timeDimension = "__time";
  public static final String marketDimension = "market";
  public static final String qualityDimension = "quality";
  public static final String placementDimension = "placement";
  public static final String placementishDimension = "placementish";
  public static final String partialNullDimension = "partial_null_column";

  public static final List<String> dimensions = Lists.newArrayList(
      marketDimension,
      qualityDimension,
      placementDimension,
      placementishDimension
  );
  public static final String indexMetric = "index";
  public static final String uniqueMetric = "uniques";
  public static final String addRowsIndexConstantMetric = "addRowsIndexConstant";
  public static final List<String> metrics = Lists.newArrayList(indexMetric, uniqueMetric, addRowsIndexConstantMetric);

  public static String dependentPostAggMetric = "dependentPostAgg";
  public static final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  public static final LongSumAggregatorFactory indexLongSum = new LongSumAggregatorFactory("index", indexMetric);
  public static final LongSumAggregatorFactory __timeLongSum = new LongSumAggregatorFactory("sumtime", timeDimension);
  public static final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", indexMetric);
  public static final String JS_COMBINE_A_PLUS_B = "function combine(a, b) { return a + b; }";
  public static final String JS_RESET_0 = "function reset() { return 0; }";
  public static final JavaScriptAggregatorFactory jsIndexSumIfPlacementishA = new JavaScriptAggregatorFactory(
      "nindex",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a, b) { if ((Array.isArray(a) && a.indexOf('a') > -1) || a === 'a') { return current + b; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getDefault()
  );
  public static final JavaScriptAggregatorFactory jsCountIfTimeGreaterThan = new JavaScriptAggregatorFactory(
      "ntimestamps",
      Arrays.asList("__time"),
      "function aggregate(current, t) { if (t > " +
      new DateTime("2011-04-01T12:00:00Z").getMillis() +
      ") { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getDefault()
  );
  public static final JavaScriptAggregatorFactory jsPlacementishCount = new JavaScriptAggregatorFactory(
      "pishcount",
      Arrays.asList("placementish", "index"),
      "function aggregate(current, a) { if (Array.isArray(a)) { return current + a.length; } else if (typeof a === 'string') { return current + 1; } else { return current; } }",
      JS_RESET_0,
      JS_COMBINE_A_PLUS_B,
      JavaScriptConfig.getDefault()
  );
  public static final HyperUniquesAggregatorFactory qualityUniques = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques"
  );
  public static final HyperUniquesAggregatorFactory qualityUniquesRounded = new HyperUniquesAggregatorFactory(
      "uniques",
      "quality_uniques",
      null,
      true,
      0
  );
  public static final CardinalityAggregatorFactory qualityCardinality = new CardinalityAggregatorFactory(
      "cardinality",
      Arrays.asList("quality"),
      false
  );
  public static final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  public static final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  public static final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  public static final ArithmeticPostAggregator addRowsIndexConstant =
      new ArithmeticPostAggregator(
          addRowsIndexConstantMetric, "+", Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
      );
  // dependent on AddRowsIndexContact postAgg
  public static final ArithmeticPostAggregator dependentPostAgg = new ArithmeticPostAggregator(
      dependentPostAggMetric,
      "+",
      Lists.newArrayList(
          constant,
          new FieldAccessPostAggregator(addRowsIndexConstantMetric, addRowsIndexConstantMetric),
          new FieldAccessPostAggregator("rows", "rows")
      )
  );

  public static final String hyperUniqueFinalizingPostAggMetric = "hyperUniqueFinalizingPostAggMetric";
  public static ArithmeticPostAggregator hyperUniqueFinalizingPostAgg = new ArithmeticPostAggregator(
      hyperUniqueFinalizingPostAggMetric,
      "+",
      Lists.newArrayList(
          new HyperUniqueFinalizingPostAggregator(uniqueMetric, uniqueMetric),
          new ConstantPostAggregator(null, 1)
      )
  );

  public static final List<AggregatorFactory> commonAggregators = Arrays.asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques
  );

  public static final double UNIQUES_9 = 9.019833517963864;
  public static final double UNIQUES_2 = 2.000977198748901d;
  public static final double UNIQUES_1 = 1.0002442201269182d;

  public static final String[] expectedFullOnIndexValues = new String[]{
      "4500.0", "6077.949111938477", "4922.488838195801", "5726.140853881836", "4698.468170166016",
      "4651.030891418457", "4398.145851135254", "4596.068244934082", "4434.630561828613",
      "6162.801361083984", "5590.292701721191", "4994.298484802246", "5179.679672241211", "6288.556800842285",
      "6025.663551330566", "5772.855537414551", "5346.517524719238", "5497.331253051758", "5909.684387207031",
      "5862.711364746094", "5958.373008728027", "5224.882194519043", "5456.789611816406", "5456.095397949219",
      "4642.481948852539", "5023.572692871094", "5155.821723937988", "5350.3723220825195", "5236.997489929199",
      "4910.097717285156", "4507.608840942383", "4659.80500793457", "5354.878845214844", "4945.796455383301",
      "6459.080368041992", "4390.493583679199", "6545.758262634277", "6922.801231384277", "6023.452911376953",
      "6812.107475280762", "6368.713348388672", "6381.748748779297", "5631.245086669922", "4976.192253112793",
      "6541.463027954102", "5983.8513107299805", "5967.189498901367", "5567.139289855957", "4863.5944747924805",
      "4681.164360046387", "6122.321441650391", "5410.308860778809", "4846.676376342773", "5333.872688293457",
      "5013.053741455078", "4836.85563659668", "5264.486434936523", "4581.821243286133", "4680.233596801758",
      "4771.363662719727", "5038.354717254639", "4816.808464050293", "4684.095504760742", "5023.663467407227",
      "5889.72257232666", "4984.973915100098", "5664.220512390137", "5572.653915405273", "5537.123138427734",
      "5980.422874450684", "6243.834693908691", "5372.147285461426", "5690.728981018066", "5827.796455383301",
      "6141.0769119262695", "6082.3237228393555", "5678.771339416504", "6814.467971801758", "6626.151596069336",
      "5833.2095947265625", "4679.222328186035", "5367.9403076171875", "5410.445640563965", "5689.197135925293",
      "5240.5018310546875", "4790.912239074707", "4992.670921325684", "4796.888023376465", "5479.439590454102",
      "5506.567192077637", "4743.144546508789", "4913.282669067383", "4723.869743347168"
  };

  public static final String[] expectedFullOnIndexValuesDesc;

  static {
    List<String> list = new ArrayList<String>(Arrays.asList(expectedFullOnIndexValues));
    Collections.reverse(list);
    expectedFullOnIndexValuesDesc = list.toArray(new String[0]);
  }

  public static final DateTime earliest = new DateTime("2011-01-12");
  public static final DateTime last = new DateTime("2011-04-15");

  public static final DateTime skippedDay = new DateTime("2011-01-21T00:00:00.000Z");

  public static final QuerySegmentSpec january = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-01-01T00:00:00.000Z/2011-02-01T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec january_20 = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-01-01T00:00:00.000Z/2011-01-20T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec firstToThird = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec secondOnly = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2011-04-02T00:00:00.000Z/P1D"))
  );
  public static final QuerySegmentSpec fullOnInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("1970-01-01T00:00:00.000Z/2120-01-01T00:00:00.000Z"))
  );
  public static final QuerySegmentSpec emptyInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("2020-04-02T00:00:00.000Z/P1D"))
  );

  public static Iterable<Object[]> transformToConstructionFeeder(Iterable<?> in)
  {
    return Iterables.transform(
        in, new Function<Object, Object[]>()
        {
          @Nullable
          @Override
          public Object[] apply(@Nullable Object input)
          {
            return new Object[]{input};
          }
        }
    );
  }

  // simple cartesian iterable
  public static Iterable<Object[]> cartesian(final Iterable... iterables)
  {
    return new Iterable<Object[]>()
    {

      @Override
      public Iterator<Object[]> iterator()
      {
        return new Iterator<Object[]>()
        {
          private final Iterator[] iterators = new Iterator[iterables.length];
          private final Object[] cached = new Object[iterables.length];

          @Override
          public boolean hasNext()
          {
            return hasNext(0);
          }

          private boolean hasNext(int index)
          {
            if (iterators[index] == null) {
              iterators[index] = iterables[index].iterator();
            }
            for (; hasMore(index); cached[index] = null) {
              if (index == iterables.length - 1 || hasNext(index + 1)) {
                return true;
              }
            }
            iterators[index] = null;
            return false;
          }

          private boolean hasMore(int index)
          {
            if (cached[index] == null && iterators[index].hasNext()) {
              cached[index] = iterators[index].next();
            }
            return cached[index] != null;
          }

          @Override
          public Object[] next()
          {
            Object[] result = Arrays.copyOf(cached, cached.length);
            cached[cached.length - 1] = null;
            return result;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException("remove");
          }
        };
      }
    };
  }

  public static <T> List<Object[]> makeQueryRunnersWithName(QueryRunnerFactory<T> factory)
      throws IOException
  {
    List runners = makeQueryRunners(factory);
    return Arrays.asList(
        new Object[] {runners.get(0), "rtIndex"},
        new Object[] {runners.get(1), "noRollupRtIndex"},
        new Object[] {runners.get(2), "mMappedTestIndex"},
        new Object[] {runners.get(3), "noRollupMMappedTestIndex"},
        new Object[] {runners.get(4), "mergedRealtimeIndex"});
  }

  public static <T> List<QueryRunner<T>> makeQueryRunners(QueryRunnerFactory<T> factory)
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final IncrementalIndex noRollupRtIndex = TestIndex.getNoRollupIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex noRollupMMappedTestIndex = TestIndex.getNoRollupMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    return ImmutableList.of(
        makeQueryRunner(factory, new IncrementalIndexSegment(rtIndex, descriptor)),
        makeQueryRunner(factory, new IncrementalIndexSegment(noRollupRtIndex, descriptor)),
        makeQueryRunner(factory, new QueryableIndexSegment(mMappedTestIndex, descriptor)),
        makeQueryRunner(factory, new QueryableIndexSegment(noRollupMMappedTestIndex, descriptor)),
        makeQueryRunner(factory, new QueryableIndexSegment(mergedRealtimeIndex, descriptor))
    );
  }

  public static <T> List<QueryRunner<T>> makeSegmentQueryRunners(QueryRunnerFactory<T> factory)
      throws IOException
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final IncrementalIndex noRollupRtIndex = TestIndex.getNoRollupIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex noRollupMMappedTestIndex = TestIndex.getNoRollupMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();
    return ImmutableList.of(
        makeSegmentQueryRunner(factory, new IncrementalIndexSegment(rtIndex, descriptor)),
        makeSegmentQueryRunner(factory, new IncrementalIndexSegment(noRollupRtIndex, descriptor)),
        makeSegmentQueryRunner(factory, new QueryableIndexSegment(mMappedTestIndex, descriptor)),
        makeSegmentQueryRunner(factory, new QueryableIndexSegment(noRollupMMappedTestIndex, descriptor)),
        makeSegmentQueryRunner(factory, new QueryableIndexSegment(mergedRealtimeIndex, descriptor))
    );
  }

  public static <T> Collection<QueryRunner<T>> makeUnionQueryRunners(
      Query<T> query,
      QueryRunnerFactory<T> factory,
      DataSource unionDataSource
  )
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();
    final QueryableIndex mMappedTestIndex = TestIndex.getMMappedTestIndex();
    final QueryableIndex mergedRealtimeIndex = TestIndex.mergedRealtimeIndex();

    return Arrays.asList(
        makeUnionQueryRunner(query, factory, new IncrementalIndexSegment(rtIndex, descriptor)),
        makeUnionQueryRunner(query, factory, new QueryableIndexSegment(mMappedTestIndex, descriptor)),
        makeUnionQueryRunner(query, factory, new QueryableIndexSegment(mergedRealtimeIndex, descriptor))
    );
  }

  /**
   * Iterate through the iterables in a synchronous manner and return each step as an Object[]
   * @param in The iterables to step through. (effectively columns)
   * @return An iterable of Object[] containing the "rows" of the input (effectively rows)
   */
  public static Iterable<Object[]> transformToConstructionFeeder(Iterable<?>... in)
  {
    if (in == null) {
      return ImmutableList.<Object[]>of();
    }
    final List<Iterable<?>> iterables = Arrays.asList(in);
    final int length = in.length;
    final List<Iterator<?>> iterators = new ArrayList<>(in.length);
    for (Iterable<?> iterable : iterables) {
      iterators.add(iterable.iterator());
    }
    return new Iterable<Object[]>()
    {
      @Override
      public Iterator<Object[]> iterator()
      {
        return new Iterator<Object[]>()
        {
          @Override
          public boolean hasNext()
          {
            int hasMore = 0;
            for (Iterator<?> it : iterators) {
              if (it.hasNext()) {
                ++hasMore;
              }
            }
            return hasMore == length;
          }

          @Override
          public Object[] next()
          {
            final ArrayList<Object> list = new ArrayList<Object>(length);
            for (Iterator<?> it : iterators) {
              list.add(it.next());
            }
            return list.toArray();
          }

          @Override
          public void remove()
          {
            throw new UOE("Remove not supported");
          }
        };
      }
    };
  }

  public static <T> QueryRunner<T> makeQueryRunner(QueryRunnerFactory<T> factory, String resourceFileName)
  {
    return makeQueryRunner(
        factory,
        new IncrementalIndexSegment(TestIndex.makeRealtimeIndex(resourceFileName, true), descriptor)
    );
  }

  public static <T> QueryRunner<T> makeSegmentQueryRunner(QueryRunnerFactory<T> factory, String resourceFileName)
  {
    return makeSegmentQueryRunner(
        factory,
        new IncrementalIndexSegment(TestIndex.makeRealtimeIndex(resourceFileName, true), descriptor)
    );
  }

  public static <T> QueryRunner<T> makeQueryRunner(QueryRunnerFactory<T> factory, Segment adapter)
  {
    return toBrokerRunner(factory.getToolchest(), makeLocalQueryRunner(factory, adapter));
  }

  @SuppressWarnings("unchecked")
  private static <T> QueryRunner<T> toBrokerRunner(QueryToolChest toolChest, QueryRunner<T> runner)
  {
    return toolChest.finalQueryDecoration(
        PostProcessingOperators.wrap(
            toolChest.finalizeResults(
                toolChest.postMergeQueryDecoration(
                    toolChest.mergeResults(
                        toolChest.preMergeQueryDecoration(
                            deserialize(runner, toolChest)
                        )
                    )
                )
            )
        )
    );
  }

  private static <T> QueryRunner<T> makeLocalQueryRunner(final QueryRunnerFactory<T> factory, final Segment segment)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        final Supplier<RowResolver> resolver = RowResolver.supplier(Arrays.asList(segment), query);
        final Query<T> resolved = query.resolveQuery(resolver, true);

        final QueryToolChest<T> toolchest = factory.getToolchest();
        final QueryRunner<T> runner = toolchest.finalQueryDecoration(
            toolchest.finalizeResults(
                toolchest.mergeResults(
                    factory.mergeRunners(
                        resolved,
                        Execs.newDirectExecutorService(),
                        ImmutableList.<QueryRunner<T>>of(makeSegmentQueryRunner(factory, segment)),
                        null
                    )
                )
            )
        );
        return runner.run(resolved, responseContext);
      }
    };
  }

  public static <T> QueryRunner<T> makeSegmentQueryRunner(final QueryRunnerFactory<T> factory, final Segment segment)
  {
    return new BySegmentQueryRunner<T>(
        factory.getToolchest(),
        segment.getIdentifier(),
        segment.getInterval().getStart(),
        new QueryRunner<T>()
        {
          @Override
          public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
          {
            // this should be done at the most outer side (see server manager).. but who cares?
            final Supplier<RowResolver> resolver = RowResolver.supplier(Arrays.asList(segment), query);
            final Query<T> resolved = query.resolveQuery(resolver, true);
            final Supplier<Object> optimizer = factory.preFactoring(
                resolved,
                Arrays.asList(segment),
                resolver,
                Execs.newDirectExecutorService()
            );
            QueryRunner<T> runner = factory.createRunner(segment, optimizer);
            return runner.run(resolved, responseContext);
          }
        }
    );
  }

  public static <T> QueryRunner<T> makeUnionQueryRunner(
      Query<T> query,
      QueryRunnerFactory<T> factory,
      Segment adapter
  )
  {
    final UnionQueryRunner<T> baseRunner = new UnionQueryRunner<>(
        new BySegmentQueryRunner<T>(
            factory.getToolchest(),
            adapter.getIdentifier(),
            adapter.getInterval().getStart(),
            factory.createRunner(adapter, null)
        )
    );
    return FluentQueryRunnerBuilder.create(factory.getToolchest(), baseRunner)
                                   .applyMergeResults()
                                   .applyPostMergeDecoration()
                                   .applyFinalizeResults()
                                   .build();
  }

  public static <T> QueryRunner<T> makeFilteringQueryRunner(
      final VersionedIntervalTimeline<Segment> timeline,
      final QueryRunnerFactory<T> factory
  )
  {

    final QueryToolChest<T> toolChest = factory.getToolchest();
    final QueryRunner<T> baseRunner = new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        List<TimelineObjectHolder<Segment>> segments = Lists.newArrayList();
        for (Interval interval : query.getIntervals()) {
          segments.addAll(timeline.lookup(interval));
        }
        List<Sequence<T>> sequences = Lists.newArrayList();
        for (TimelineObjectHolder<Segment> holder : toolChest.filterSegments(query, segments)) {
          Segment segment = holder.getObject().getChunk(0).getObject();
          Query<T> running = query.withQuerySegmentSpec(
              new SpecificSegmentSpec(
                  new SegmentDescriptor(
                      "foo",
                      holder.getInterval(),
                      holder.getVersion(),
                      0
                  )
              )
          );
          sequences.add(factory.createRunner(segment, null).run(running, responseContext));
        }
        Comparator<T> ordering = query.getMergeOrdering(query.estimatedInitialColumns());
        return new MergeSequence<>(ordering, Sequences.simple(sequences));
      }
    };
    return FluentQueryRunnerBuilder.create(toolChest, baseRunner)
                                   .applyPreMergeDecoration()
                                   .applyMergeResults()
                                   .applyPostMergeDecoration()
                                   .applyFinalizeResults()
                                   .build();
  }

  public static <T> QueryRunner<T> toMergeRunner(
      final QueryRunnerFactory<T> factory,
      final QueryRunner<T> runner,
      final Query<T> query
  )
  {
    QueryToolChest<T> toolChest = factory.getToolchest();

    QueryRunner<T> resolved = deserialize(runner, toolChest);
    if (query.getDataSource() instanceof QueryDataSource) {
      throw new UnsupportedOperationException("sub-query");
    }
    resolved = toolChest.postMergeQueryDecoration(toolChest.mergeResults(toolChest.preMergeQueryDecoration(resolved)));
    resolved = toolChest.finalizeResults(resolved);
    return toolChest.finalQueryDecoration(resolved);
  }

  private static <T> QueryRunner<T> deserialize(
      final QueryRunner<T> runner,
      final QueryToolChest<T> toolChest
  )
  {
    return new QueryRunner<T>()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        Query<T> prepared = query.toLocalQuery();
        Function manipulatorFn = toolChest.makePreComputeManipulatorFn(prepared, MetricManipulatorFns.deserializing());
        if (BaseQuery.isBySegment(prepared)) {
          manipulatorFn = BySegmentResultValue.applyAll(manipulatorFn);
        }
        return Sequences.map(runner.run(prepared, responseContext), manipulatorFn);
      }
    };
  }


  @SuppressWarnings("unchecked")
  public List<Map<String, Object>> runTabularQuery(Query query)
  {
    return io.druid.common.utils.Sequences.toList(query.run(getSegmentWalker(), Maps.<String, Object>newHashMap()));
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> runQuery(Query query)
  {
    return runQuery(query, getSegmentWalker());
  }

  protected TestQuerySegmentWalker getSegmentWalker()
  {
    return TestIndex.segmentWalker;
  }
}
