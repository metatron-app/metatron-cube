package io.druid.query.aggregation.median;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.*;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

@RunWith(Parameterized.class)
public class DruidTDigestGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    final StupidPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocate(1024 * 1024);
          }
        }
    );

    final GroupByQueryConfig config = new GroupByQueryConfig();
    config.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByQueryEngine engine = new GroupByQueryEngine(configSupplier, pool);

    final GroupByQueryRunnerFactory factory = new GroupByQueryRunnerFactory(
        engine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        configSupplier,
        new GroupByQueryQueryToolChest(
            configSupplier, mapper, engine, pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        pool
    );

    GroupByQueryConfig singleThreadedConfig = new GroupByQueryConfig()
    {
      @Override
      public boolean isSingleThreaded()
      {
        return true;
      }
    };
    singleThreadedConfig.setMaxIntermediateRows(10000);

    final Supplier<GroupByQueryConfig> singleThreadedConfigSupplier = Suppliers.ofInstance(singleThreadedConfig);
    final GroupByQueryEngine singleThreadEngine = new GroupByQueryEngine(singleThreadedConfigSupplier, pool);

    final GroupByQueryRunnerFactory singleThreadFactory = new GroupByQueryRunnerFactory(
        singleThreadEngine,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        singleThreadedConfigSupplier,
        new GroupByQueryQueryToolChest(
            singleThreadedConfigSupplier, mapper, singleThreadEngine, pool,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        pool
    );


    final Function<Object, Object[]> function = new Function<Object, Object[]>()
    {
      @Override
      public Object[] apply(@Nullable Object input)
      {
        return new Object[]{factory, input};
      }
    };

    return Lists.newArrayList(
        Iterables.concat(
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(factory),
                function
            ),
            Iterables.transform(
                QueryRunnerTestHelper.makeQueryRunners(singleThreadFactory),
                function
            )
        )
    );
  }

  public DruidTDigestGroupByQueryTest(GroupByQueryRunnerFactory factory, QueryRunner runner)
  {
    this.factory = factory;
    this.runner = runner;
  }

  @Test
  public void testGroupByWithDruidTDigestAgg()
  {
    DruidTDigestAggregatorFactory aggFactory = new DruidTDigestAggregatorFactory(
        "digest",
        "index",
        50
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            Lists.newArrayList(
                QueryRunnerTestHelper.rowsCount,
                aggFactory
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new DruidTDigestMedianPostAggregator("median", "digest")
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "median", 951.2986450195312f
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    Assert.assertEquals("T Digest median",
        expectedResults.get(0).getFloatMetric("median"), results.iterator().next().getFloatMetric("median"), 5);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithSameNameComplexPostAgg()
  {
    DruidTDigestAggregatorFactory aggFactory = new DruidTDigestAggregatorFactory(
        "quantile",
        "index",
        10
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            Lists.newArrayList(
                QueryRunnerTestHelper.rowsCount,
                aggFactory
            )
        )
        .setPostAggregatorSpecs(
            Arrays.<PostAggregator>asList(
                new DruidTDigestMedianPostAggregator("quantile", "quantile")
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 880.9881f
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "approx-histo");
  }
}
