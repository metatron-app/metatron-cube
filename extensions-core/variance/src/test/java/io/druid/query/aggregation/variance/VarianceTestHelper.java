package io.druid.query.aggregation.variance;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class VarianceTestHelper extends QueryRunnerTestHelper
{
  public static final String indexVarianceMetric = "index_var";

  public static final VarianceAggregatorFactory indexVarianceAggr = new VarianceAggregatorFactory(
      indexVarianceMetric,
      indexMetric
  );

  public static final String stddevOfIndexMetric = "index_stddev";

  public static final ArithmeticPostAggregator stddevOfIndexAggr = new ArithmeticPostAggregator(
      stddevOfIndexMetric, ArithmeticPostAggregator.UnaryOp.SQRT.name(),
      Arrays.<PostAggregator>asList(
          new VarianceFinalizingPostAggregator(indexVarianceMetric, indexVarianceMetric)
      )
  );

  public static final List<AggregatorFactory> commonPlusVarAggregators = Arrays.asList(
      rowsCount,
      indexDoubleSum,
      qualityUniques,
      indexVarianceAggr
  );

  public static class RowBuilder
  {
    private final String[] names;
    private final List<Row> rows = Lists.newArrayList();

    public RowBuilder(String[] names)
    {
      this.names = names;
    }

    public RowBuilder add(final String timestamp, Object... values)
    {
      rows.add(build(timestamp, values));
      return this;
    }

    public List<Row> build()
    {
      try {
        return Lists.newArrayList(rows);
      }
      finally {
        rows.clear();
      }
    }

    public Row build(final String timestamp, Object... values)
    {
      Preconditions.checkArgument(names.length == values.length);

      Map<String, Object> theVals = Maps.newHashMap();
      for (int i = 0; i < values.length; i++) {
        theVals.put(names[i], values[i]);
      }
      DateTime ts = new DateTime(timestamp);
      return new MapBasedRow(ts, theVals);
    }
  }
}
