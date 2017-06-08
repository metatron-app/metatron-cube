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
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.DelegatingYieldingAccumulator;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.common.logger.Logger;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRow;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.mutable.MutableLong;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class HoltWintersPostProcessor extends PostProcessingOperator.Abstract
{
  private static final Logger LOG = new Logger(HoltWintersPostProcessor.class);

  private final double alpha;
  private final double beta;
  private final double gamma;
  private final int period;
  private final HoltWintersModel.SeasonalityType seasonalityType;
  private final boolean pad;
  private final List<String> columns;
  private final int numPrediction;
  private final int limit;

  @JsonCreator
  public HoltWintersPostProcessor(
      @JsonProperty("alpha") Double alpha,
      @JsonProperty("beta") Double beta,
      @JsonProperty("gamma") Double gamma,
      @JsonProperty("period") Integer period,
      @JsonProperty("seasonalityType") HoltWintersModel.SeasonalityType seasonalityType,
      @JsonProperty("pad") Boolean pad,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("numPrediction") Integer numPrediction,
      @JsonProperty("trainLastN") Integer trainLastN
  )
  {
    this.alpha = alpha == null ? HoltWintersModel.DEFAULT_ALPHA : alpha;
    this.beta = beta == null ? HoltWintersModel.DEFAULT_BETA : beta;
    this.gamma = gamma == null ? HoltWintersModel.DEFAULT_GAMMA : gamma;
    this.period = period == null ? HoltWintersModel.DEFAULT_PERIOD : period;
    this.seasonalityType = seasonalityType == null ? HoltWintersModel.DEFAULT_SEASONALITY_TYPE : seasonalityType;
    this.pad = pad == null ? HoltWintersModel.DEFAULT_PAD : pad;
    this.columns = columns;
    this.numPrediction = numPrediction == null ? 32 : numPrediction;
    this.limit = trainLastN == null ? 1024 : trainLastN;
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
          // this is used for quick calculation of prediction only
          final LimitedQueue<Number>[] numbers = makeReservoir(numericColumns.length);
          baseRunner.run(query, responseContext).accumulate(
              null, new Accumulator<Object, StreamQueryRow>()
              {
                @Override
                public Object accumulate(Object accumulated, StreamQueryRow in)
                {
                  for (int i = 0; i < numericColumns.length; i++) {
                    Object value = in.get(numericColumns[i]);
                    if (value instanceof Number) {
                      numbers[i].add((Number) value);
                    }
                  }
                  return null;
                }
              }
          );
          return Sequences.simple(Arrays.asList(makeArrayedPrediction(numericColumns, numbers)));

        } else if (query instanceof GroupByQuery) {
          final GroupByQuery groupBy = (GroupByQuery) query;
          final String[] dimensions = Lists.transform(groupBy.getDimensions(), DimensionSpec.OUTPUT_NAME)
                                           .toArray(new String[0]);

          final Map<ObjectArray<Object>, Object> numbersMap = Maps.newHashMap();
          final MutableLong lastTimestamp = new MutableLong();
          final Sequence<Row> sequence = baseRunner.run(groupBy, responseContext);
          Sequence<Row> tapping = new Sequence<Row>()
          {
            @Override
            public <OutType> OutType accumulate(
                OutType initValue, final Accumulator<OutType, Row> accumulator
            )
            {
              return sequence.accumulate(
                  initValue, new Accumulator<OutType, Row>()
                  {
                    @Override
                    public OutType accumulate(OutType accumulated, Row in)
                    {
                      for (int i = 0; i < numericColumns.length; i++) {
                        final Object[] values = new Object[dimensions.length];
                        for (int d = 0; d < dimensions.length; d++) {
                          values[d] = internIfString(in.getRaw(dimensions[d]));
                        }
                        final ObjectArray key = new ObjectArray(values);
                        LimitedQueue<Number>[] numbers = (LimitedQueue<Number>[]) numbersMap.get(key);
                        if (numbers == null) {
                          numbersMap.put(key, numbers = makeReservoir(numericColumns.length));
                        }
                        Object value = in.getRaw(numericColumns[i]);
                        if (value instanceof Number) {
                          numbers[i].add((Number) value);
                        }
                      }
                      lastTimestamp.setValue(in.getTimestampFromEpoch());
                      return accumulator.accumulate(accumulated, in);
                    }
                  }
              );
            }

            @Override
            public <OutType> Yielder<OutType> toYielder(
                OutType initValue, YieldingAccumulator<OutType, Row> accumulator
            )
            {
              return sequence.toYielder(
                  initValue, new DelegatingYieldingAccumulator<OutType, Row>(accumulator)
                  {
                    @Override
                    public OutType accumulate(OutType accumulated, Row in)
                    {
                      for (int i = 0; i < numericColumns.length; i++) {
                        final Object[] values = new Object[dimensions.length];
                        for (int d = 0; d < dimensions.length; d++) {
                          values[d] = internIfString(in.getRaw(dimensions[d]));
                        }
                        final ObjectArray key = new ObjectArray(values);
                        LimitedQueue<Number>[] numbers = (LimitedQueue<Number>[]) numbersMap.get(key);
                        if (numbers == null) {
                          numbersMap.put(key, numbers = makeReservoir(numericColumns.length));
                        }
                        Object value = in.getRaw(numericColumns[i]);
                        if (value instanceof Number) {
                          numbers[i].add((Number) value);
                        }
                      }
                      lastTimestamp.setValue(in.getTimestampFromEpoch());
                      return super.accumulate(accumulated, in);
                    }
                  }
              );
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
                      groupBy.getGranularity()
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
  private LimitedQueue<Number>[] makeReservoir(int length)
  {
    final LimitedQueue<Number>[] numbers = (LimitedQueue<Number>[]) Array.newInstance(
        LimitedQueue.class,
        columns.size()
    );
    for (int i = 0; i < length; i++) {
      numbers[i] = new LimitedQueue(limit);
    }
    return numbers;
  }

  private Object internIfString(Object value)
  {
    return value instanceof String ? ((String)value).intern() : value;
  }

  private boolean hasPrediction(int history)
  {
    return history >= period * 2;
  }

  private Map<String, Object> makeArrayedPrediction(String[] columnNames, LimitedQueue<Number>[] numbers)
  {
    Map<String, Object> predictions = Maps.newLinkedHashMap();
    HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    for (int i = 0; i < columnNames.length; i++) {
      if (hasPrediction(numbers[i].size())) {
        double[][] prediction = Predictions.predict(model, asArray(numbers[i]), numPrediction, 95, true);
        predictions.put(columnNames[i], prediction);
      }
    }
    return predictions;
  }

  @SuppressWarnings("unchecked")
  private List<Row> makeRowedPrediction(
      String[] metrics,
      String[] dimensions,
      Map<ObjectArray<Object>, Object> numbersMap,
      long lastTimestamp,
      QueryGranularity granularity
  )
  {
    List<Row> rows = Lists.newArrayListWithExpectedSize(numPrediction);
    try {
      HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
      for (Map.Entry<ObjectArray<Object>, Object> entry : numbersMap.entrySet()) {
        LimitedQueue<Number>[] numbers = (LimitedQueue<Number>[]) entry.getValue();
        double[][][] predictions = new double[metrics.length][][];
        for (int i = 0; i < metrics.length; i++) {
          if (hasPrediction(numbers[i].size())) {
            double[] tsData = asArray(numbers[i]);
            predictions[i] = Predictions.predict(model, tsData, numPrediction, 95, true);
            LOG.info("%s : %s", Arrays.toString(tsData), Arrays.toString(predictions[i][1]));
          }
        }
        entry.setValue(predictions);
      }
      for (int p = 0; p < numPrediction; p++) {
        lastTimestamp = granularity.next(lastTimestamp);
        for (Map.Entry<ObjectArray<Object>, Object> entry : numbersMap.entrySet()) {
          Map<String, Object> row = Maps.newLinkedHashMap();
          final ObjectArray<Object> key = entry.getKey();
          for (int i = 0; i < dimensions.length; i++) {
            row.put(dimensions[i], key.array()[i]);
          }
          final double[][][] predictions = (double[][][]) entry.getValue();
          for (int i = 0; i < metrics.length; i++) {
            if (predictions[i] != null) {
              row.put(metrics[i], predictions[i][p]);
            }
          }
          rows.add(new MapBasedRow(granularity.toDateTime(lastTimestamp), row));
        }
      }
    }
    catch (Exception e) {
      LOG.warn(e, "Failed");
      throw Throwables.propagate(e);
    }
    return rows;
  }

  private double[] asArray(LimitedQueue<Number> numbers) {
    int i = 0;
    double[] array = new double[numbers.size()];
    for (Number number : numbers) {
      array[i++] = number.doubleValue();
    }
    return array;
  }

  private static class LimitedQueue<T> implements Iterable<T>
  {
    private final List<T> queue = Lists.newArrayList();
    private final int limit;
    private int index = -1;

    private LimitedQueue(int limit) {this.limit = limit;}

    public void add(T object)
    {
      if (queue.size() < limit) {
        queue.add(object);
      } else {
        queue.set(index = (index + 1) % limit, object);
      }
    }

    public int size()
    {
      return queue.size();
    }

    @Override
    public Iterator<T> iterator()
    {
      if (queue.size() < limit || index == limit - 1) {
        return queue.iterator();
      }
      return Iterables.concat(queue.subList(index + 1, limit), queue.subList(0, index + 1)).iterator();
    }

    public List<T> asList()
    {
      return ImmutableList.copyOf(this);
    }
  }
}
