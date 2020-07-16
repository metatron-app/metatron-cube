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

package io.druid.query.aggregation.corr;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.common.DateTimes;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DummyQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QueryRunners;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.aggregation.GenericMaxAggregatorFactory;
import io.druid.query.aggregation.GenericSumAggregatorFactory;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.VirtualColumn;
import org.apache.commons.math3.distribution.TDistribution;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("regression")
public class RegressionQuery extends BaseQuery<Object[]>
    implements Query.RewritingQuery<Object[]>, Query.ArrayOutput
{
  private static final Logger LOG = new Logger(RegressionQuery.class);
  private static final double DEFAULT_CONFIDENCE = 0.95f;

  private final List<VirtualColumn> virtualColumns;
  private final boolean exponential;  // todo
  private final String y;
  private final int numPredict;
  private final Period predictPeriod;
  private final double confidence;
  private final Double threshold;

  public RegressionQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("virtualColumns") List<VirtualColumn> virtualColumns,
      @JsonProperty("exponential") boolean exponential,
      @JsonProperty("y") String y,
      @JsonProperty("numPredict") int numPredict,
      @JsonProperty("predictGranulariry") Period predictPeriod,
      @JsonProperty("confidence") Double confidence,
      @JsonProperty("threshold") Double threshold,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.virtualColumns = virtualColumns == null ? ImmutableList.<VirtualColumn>of() : virtualColumns;
    this.exponential = exponential;
    this.y = Preconditions.checkNotNull(y, "y should not be null");
    this.numPredict = numPredict;
    this.predictPeriod = predictPeriod;
    this.confidence = confidence == null ? DEFAULT_CONFIDENCE : confidence;
    if (confidence != null) {
      Preconditions.checkArgument(0 <= confidence && confidence < 1, "invalid confidence %f", confidence);
    }
    this.threshold = threshold;
  }

  @Override
  public String getType()
  {
    return "regression";
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<VirtualColumn> getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public boolean isExponential()
  {
    return exponential;
  }

  @JsonProperty
  public String getY()
  {
    return y;
  }

  @JsonProperty
  public int getNumPredict()
  {
    return numPredict;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Period getPredictPeriod()
  {
    return predictPeriod;
  }

  @JsonProperty
  public double getConfidence()
  {
    return confidence;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Double getThreshold()
  {
    return threshold;
  }

  @Override
  public Query<Object[]> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new RegressionQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        virtualColumns,
        exponential,
        y,
        numPredict,
        predictPeriod,
        confidence,
        threshold,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public Query<Object[]> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new RegressionQuery(
        getDataSource(),
        spec,
        virtualColumns,
        exponential,
        y,
        numPredict,
        predictPeriod,
        confidence,
        threshold,
        getContext()
    );
  }

  @Override
  public Query<Object[]> withDataSource(DataSource dataSource)
  {
    return new RegressionQuery(
        dataSource,
        getQuerySegmentSpec(),
        virtualColumns,
        exponential,
        y,
        numPredict,
        predictPeriod,
        confidence,
        threshold,
        getContext()
    );
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    List<String> columns = Arrays.asList("__time", "y", "predict");
    if (confidence > 0) {
      columns = GuavaUtils.concat(columns, "upper", "lower");
    }
    return columns;
  }

  @Override
  public Query rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    final Map<String, Object> context = BaseQuery.copyContextForMeta(this);
    final TimeseriesQuery regression = new TimeseriesQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        null,
        Granularities.ALL,
        virtualColumns,
        Arrays.asList(
            new PearsonAggregatorFactory("corr", "__time", y, null, null),
            GenericMaxAggregatorFactory.of("time_max", "__time")
        ),
        Arrays.asList(
            new Regressions.RegrSlope("$slope", "corr"),
            new Regressions.RegrIntercept("$intercept", "corr"),
            new Regressions.RegrCount("$count", "corr"),
            new Regressions.RegrAvgX("$avgx", "corr"),
            new Regressions.RegrSXX("$varx", "corr")
        ),
        null,
        null,
        null,
        null,
        context
    );
    final Row result1 = Sequences.only(QueryRunners.run(regression, segmentWalker));

    final DateTime timeMax = DateTimes.utc(result1.getLong("time_max"));
    final double slope = result1.getDouble("$slope");
    final double intercept = result1.getDouble("$intercept");
    final long count = result1.getLong("$count");
    final double avgx = result1.getDouble("$avgx");
    final double varx = result1.getDouble("$varx");

    final String expression = String.format(
        "pow(\"%s\" - (%sD + %sD * cast(__time, 'long')), 2)", y,
        BigDecimal.valueOf(intercept).toPlainString(),
        BigDecimal.valueOf(slope).toPlainString()
    );
    double coef = -1;
    if (confidence > 0) {
      final TimeseriesQuery query = regression.withPostAggregatorSpecs(null)
                                              .withAggregatorSpecs(Arrays.asList(
                                                  GenericSumAggregatorFactory.expr("$sse", expression, ValueDesc.DOUBLE)
                                              ));
      final Row result2 = Sequences.only(QueryRunners.run(query, segmentWalker));

      final double sse = result2.getDouble("$sse");
      final double se = Math.sqrt(sse / (double) (count - 2));
      final double alpha = 1.0D - (1.0D - confidence) / 2D;
      final double tinv = new TDistribution(count - 2).inverseCumulativeProbability(alpha);
      coef = tinv * se;
    }

    final Sequence<Object[]> sequence = Sequences.map(
        QueryRunners.run(
            new StreamQuery(
                getDataSource(), getQuerySegmentSpec(), isDescending(), null, Arrays.asList("__time", y), virtualColumns,
                null, null, null, null, context
            ), segmentWalker),
        input -> {
          final Object[] row = Arrays.copyOf(input, input.length + 1);
          row[input.length] = slope * ((Number) row[0]).longValue() + intercept;
          return row;
        }
    );

    final Period period = predictPeriod == null ? Period.parse("P1D") : predictPeriod;
    final List<Object[]> result = Lists.newArrayList();

    DateTime predictTime = timeMax;
    for (int i = 0; i < numPredict; i++) {
      predictTime = predictTime.withPeriodAdded(period, 1);
      double predict = slope * predictTime.getMillis() + intercept;
      if (confidence > 0) {
        double delta = coef * stddev(predictTime.getMillis(), count, avgx, varx);
        double upper = predict + delta;
        double lower = predict - delta;
        result.add(new Object[]{predictTime.getMillis(), null, predict, lower, upper});
      } else {
        result.add(new Object[]{predictTime.getMillis(), null, predict});
      }
    }

    if (!exponential && threshold != null) {
      double rul = slope == 0 ? threshold == intercept ? 0 : Double.POSITIVE_INFINITY : (threshold - intercept) / slope;
      DateTime dateTime = DateTimes.utc((long) rul);
      long days = new Duration(dateTime.getMillis() - timeMax.getMillis()).getStandardDays();
      result.add(new Object[]{dateTime.getMillis(), dateTime, days});
    }
    return DummyQuery.of(Sequences.concat(sequence, Sequences.simple(result)));
  }

  private static double stddev(double x, double count, double xavg, double xvar)
  {
    return Math.sqrt(1.0D / count + Math.pow(x - xavg, 2D) / xvar);
  }
}
