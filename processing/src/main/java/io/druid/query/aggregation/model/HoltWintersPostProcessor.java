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

package io.druid.query.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.logger.Logger;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRow;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math3.optim.SimpleBounds;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class HoltWintersPostProcessor extends PostProcessingOperator.Abstract
{
  private static final Logger LOG = new Logger(HoltWintersPostProcessor.class);

  private static final int DEFAULT_USE_LAST_N = 4096;
  private static final int DEFAULT_NUM_PREDICTION = 32;
  private static final int DEFAULT_CONFIDENCE = 95;

  private final double alpha;
  private final double beta;
  private final double gamma;
  private final int period;
  private final HoltWintersModel.SeasonalityType seasonalityType;
  private final boolean pad;
  private final List<String> columns;
  private final int numPrediction;
  private final int limit;
  private final int confidence;
  private final List<PostAggregator> postAggregations;

  private final String timeColumn;
  private final Granularity timeGranularity;
  private final DateTimeFormatter dateTimeFormatter;

  private final SimpleBounds bounds;

  @JsonCreator
  public HoltWintersPostProcessor(
      @JsonProperty("alpha") Double alpha,
      @JsonProperty("beta") Double beta,
      @JsonProperty("gamma") Double gamma,
      @JsonProperty("period") Integer period,
      @JsonProperty("seasonalityType") HoltWintersModel.SeasonalityType seasonalityType,
      @JsonProperty("pad") Boolean pad,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("useLastN") Integer useLastN,
      @JsonProperty("numPrediction") Integer numPrediction,
      @JsonProperty("confidence") Integer confidence,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations,
      @JsonProperty("timeColumn") String timeColumn,
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("timeLocale") String timeLocale,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("timeGranularity") Granularity timeGranularity
  )
  {
    this.alpha = alpha == null ? HoltWintersModel.DEFAULT_ALPHA : alpha;
    this.beta = beta == null ? HoltWintersModel.DEFAULT_BETA : beta;
    this.gamma = gamma == null ? HoltWintersModel.DEFAULT_GAMMA : gamma;
    this.period = period == null ? HoltWintersModel.DEFAULT_PERIOD : period;
    this.seasonalityType = seasonalityType == null ? HoltWintersModel.DEFAULT_SEASONALITY_TYPE : seasonalityType;
    this.pad = pad == null ? HoltWintersModel.DEFAULT_PAD : pad;
    this.columns = columns;
    this.limit = useLastN == null ? DEFAULT_USE_LAST_N : useLastN;
    this.numPrediction = numPrediction == null ? DEFAULT_NUM_PREDICTION : numPrediction;
    this.confidence = confidence == null ? DEFAULT_CONFIDENCE : confidence;
    this.postAggregations = postAggregations == null ? ImmutableList.<PostAggregator>of() : postAggregations;
    this.timeColumn = timeColumn;
    this.timeGranularity = timeGranularity;
    this.bounds = new SimpleBounds(
        new double[]{alpha == null ? 0 : alpha, beta == null ? 0 : beta, gamma == null ? 0 : gamma},
        new double[]{alpha == null ? 1 : alpha, beta == null ? 1 : beta, gamma == null ? 1 : gamma}
    );
    Preconditions.checkArgument(
        timeColumn == null && timeFormat == null && timeGranularity == null ||
        timeColumn != null && timeFormat != null && timeGranularity != null
    );
    this.dateTimeFormatter = timeFormat != null ? JodaUtils.toTimeFormatter(timeFormat, timeLocale, timeZone) : null;
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final String[] numericColumns = columns.toArray(new String[columns.size()]);
        if (query instanceof StreamQuery) {
          Preconditions.checkArgument(
              dateTimeFormatter == null,
              "Custom time column is supported only by group-by query"
          );
          final Granularity granularity = ((StreamQuery) query).getGranularity();
          // this is used for quick calculation of prediction only
          final BoundedTimeseries[] numbers = makeReservoir(numericColumns.length, granularity);
          baseRunner.run(query, responseContext).accumulate(
              null, new Accumulator<Object, StreamQueryRow>()
              {
                @Override
                public Object accumulate(Object accumulated, StreamQueryRow in)
                {
                  final DateTime timestamp = DateTimes.utc(in.getTimestamp());
                  for (int i = 0; i < numericColumns.length; i++) {
                    Object value = in.get(numericColumns[i]);
                    if (value instanceof Number) {
                      numbers[i].add(((Number) value).doubleValue(), timestamp);
                    }
                  }
                  return null;
                }
              }
          );
          return Sequences.simple(Arrays.asList(makeArrayedPrediction(numericColumns, numbers)));

        } else if (query instanceof GroupByQuery) {
          final GroupByQuery groupBy = (GroupByQuery) query;
          final Granularity granularity = timeColumn == null ? groupBy.getGranularity() : timeGranularity;
          List<String> groupByColumns = DimensionSpecs.toOutputNames(groupBy.getDimensions());
          if (timeColumn != null) {
            groupByColumns.remove(timeColumn);
          }
          final String[] dimensions = groupByColumns.toArray(new String[0]);

          final Map<ObjectArray<Object>, BoundedTimeseries[]> numbersMap = Maps.newHashMap();
          final MutableLong lastTimestamp = new MutableLong();
          final Sequence<Row> sequence = baseRunner.run(groupBy, responseContext);

          Sequence<Row> tapping = new Sequences.PeekingSequence<Row>(sequence)
          {
            @Override
            protected final Row peek(final Row in)
            {
              final DateTime timestamp;
              if (dateTimeFormatter != null) {
                timestamp = dateTimeFormatter.parseDateTime(String.valueOf(in.getRaw(timeColumn)));
              } else {
                timestamp = in.getTimestamp();
              }
              for (int i = 0; i < numericColumns.length; i++) {
                final Object[] values = new Object[dimensions.length];
                for (int d = 0; d < dimensions.length; d++) {
                  values[d] = internIfString(in.getRaw(dimensions[d]));
                }
                final ObjectArray key = new ObjectArray(values);
                BoundedTimeseries[] numbers = (BoundedTimeseries[]) numbersMap.get(key);
                if (numbers == null) {
                  numbersMap.put(key, numbers = makeReservoir(numericColumns.length, granularity));
                }
                Object value = in.getRaw(numericColumns[i]);
                if (value instanceof Number) {
                  numbers[i].add(((Number) value).doubleValue(), timestamp);
                }
              }
              lastTimestamp.setValue(timestamp.getMillis());
              return in;
            }
          };

          Supplier<Sequence<Row>> supplier = new Supplier<Sequence<Row>>()
          {
            @Override
            public Sequence<Row> get()
            {
              LOG.info("Calculating %d predictions.. ", numPrediction);
              return Sequences.simple(
                  makeRowedPrediction(
                      numericColumns,
                      dimensions,
                      numbersMap,
                      lastTimestamp.longValue(),
                      granularity
                  )
              );
            }
          };
          return Sequences.concat(tapping, new LazySequence<Row>(supplier));
        } else {
          LOG.info("Not supported query type " + query.getType());
          return baseRunner.run(query, responseContext);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private BoundedTimeseries[] makeReservoir(int length, Granularity granularity)
  {
    final BoundedTimeseries[] numbers = (BoundedTimeseries[]) Array.newInstance(
        BoundedTimeseries.class,
        columns.size()
    );
    for (int i = 0; i < length; i++) {
      numbers[i] = new BoundedTimeseries(limit, granularity);
    }
    return numbers;
  }

  private Object internIfString(Object value)
  {
    return value instanceof String ? ((String) value).intern() : value;
  }

  private boolean hasPrediction(int history)
  {
    return history >= period * 2;
  }

  private Map<String, Object> makeArrayedPrediction(String[] columnNames, BoundedTimeseries[] numbers)
  {
    Map<String, Object> predictions = Maps.newLinkedHashMap();
    HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    for (int i = 0; i < columnNames.length; i++) {
      if (hasPrediction(numbers[i].size())) {
        double[] tsData = numbers[i].asArray();
        double[][] prediction = Predictions.predict(model, bounds, tsData, numPrediction, confidence, true);
        predictions.put(columnNames[i], prediction);
      }
    }
    return predictions;
  }

  @SuppressWarnings("unchecked")
  private List<Row> makeRowedPrediction(
      String[] metrics,
      String[] dimensions,
      Map<ObjectArray<Object>, BoundedTimeseries[]> numbersMap,
      long lastTimestamp,
      Granularity granularity
  )
  {
    DateTime timestamp = granularity.toDateTime(lastTimestamp);
    List<Row> rows = Lists.newArrayListWithExpectedSize(numPrediction);

    Map<ObjectArray<Object>, Pair<double[][], double[][][]>> predictionMap = Maps.newHashMap();
    try {
      HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
      for (Map.Entry<ObjectArray<Object>, BoundedTimeseries[]> entry : numbersMap.entrySet()) {
        BoundedTimeseries[] numbers = entry.getValue();
        double[][] params = new double[metrics.length][];
        double[][][] predictions = new double[metrics.length][][];
        for (int i = 0; i < metrics.length; i++) {
          if (hasPrediction(numbers[i].size())) {
            double[] tsData = numbers[i].asArray();
            HoltWintersModel optimized = Predictions.optimize(model, tsData, bounds);
            params[i] = new double[]{optimized.alpha(), optimized.beta(), optimized.gamma()};
            predictions[i] = Predictions.predictWithModel(optimized, tsData, numPrediction, confidence, true);
          }
        }
        predictionMap.put(entry.getKey(), Pair.<double[][], double[][][]>of(params, predictions));
      }
      for (int p = 0; p < numPrediction; p++) {
        timestamp = granularity.bucketEnd(timestamp);
        for (Map.Entry<ObjectArray<Object>, Pair<double[][], double[][][]>> entry : predictionMap.entrySet()) {
          Map<String, Object> row = Maps.newLinkedHashMap();
          final ObjectArray<Object> key = entry.getKey();
          for (int i = 0; i < dimensions.length; i++) {
            row.put(dimensions[i], key.array()[i]);
          }
          final double[][] params = entry.getValue().lhs;
          final double[][][] predictions = entry.getValue().rhs;
          for (int i = 0; i < metrics.length; i++) {
            if (predictions[i] != null) {
              row.put(metrics[i], predictions[i][p]);
              row.put(metrics[i] + ".params", params[i]);
            }
          }
          for (PostAggregator postAggregator : postAggregations) {
            row.put(postAggregator.getName(), postAggregator.compute(timestamp, row));
          }
          rows.add(new PredictedRow(timestamp, row));
        }
      }
    }
    catch (Exception e) {
      LOG.warn(e, "Failed");
      throw Throwables.propagate(e);
    }
    return rows;
  }

  private static class BoundedTimeseries
  {
    private final int limit;
    private double[] values;
    private int index;
    private boolean exceeded;

    private final Granularity granularity;
    private long prevTimestamp;
    private Long bucketEnd;

    private BoundedTimeseries(int limit, Granularity granularity)
    {
      this.limit = Math.max(limit, 32);
      this.values = new double[32];
      this.granularity = granularity;
    }

    public void add(double value, DateTime currentTime)
    {
      if (bucketEnd == null || bucketEnd == currentTime.getMillis()) {
        bucketEnd = granularity.bucketEnd(currentTime).getMillis();
      } else if (currentTime.isAfter(bucketEnd)) {
        final int count = countEmptyTo(currentTime);
        if (count > 0) {
          double lastValue = lastValue();
          double delta = (value - lastValue) / count;
          for (int i = 0; i < count; i++) {
            _add(lastValue + delta * i);  // linear...
          }
        }
        bucketEnd = granularity.bucketEnd(currentTime).getMillis();
      } else if (currentTime.isBefore(prevTimestamp)) {
        throw new ISE("Input data is not time-sorted");
      }
      prevTimestamp = currentTime.getMillis();
      _add(value);
    }

    private int countEmptyTo(DateTime currentTime)
    {
      DateTime newStart = granularity.bucketStart(currentTime);
      if (newStart.getMillis() == bucketEnd) {
        return 0;
      }
      DateTime from = new DateTime(bucketEnd, currentTime.getChronology());
      return Iterables.size(granularity.getIterable(Intervals.of(from, newStart)));
    }

    private double lastValue()
    {
      return index > 0 ? values[index - 1] : values[values.length - 1];
    }

    private void _add(double value)
    {
      if (values.length < limit && index == values.length - 1) {
        double[] extended = new double[Math.min(limit, values.length * 2)];
        System.arraycopy(values, 0, extended, 0, values.length);
        values = extended;
      }
      values[index++] = value;
      index = index % values.length;
      exceeded |= index == 0;
    }

    public int size()
    {
      return exceeded ? values.length : index;
    }

    public double[] asArray()
    {
      if (!exceeded) {
        return Arrays.copyOfRange(values, 0, index);
      }
      if (index == 0) {
        return values;
      }
      final double[] result = new double[values.length];
      System.arraycopy(values, index, result, 0, values.length - index);
      System.arraycopy(values, 0, result, values.length - index, index);
      return result;
    }
  }

  // just to deliver marking
  public static class PredictedRow extends MapBasedRow
  {
    public PredictedRow(DateTime timestamp, Map<String, Object> event)
    {
      super(timestamp, event);
    }
  }
}
