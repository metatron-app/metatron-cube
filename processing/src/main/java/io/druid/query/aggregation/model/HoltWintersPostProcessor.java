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
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.select.StreamQuery;
import io.druid.query.select.StreamQueryRow;
import org.apache.commons.lang.mutable.MutableLong;

import java.lang.reflect.Array;
import java.util.Arrays;
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

  @JsonCreator
  public HoltWintersPostProcessor(
      @JsonProperty("alpha") Double alpha,
      @JsonProperty("beta") Double beta,
      @JsonProperty("gamma") Double gamma,
      @JsonProperty("period") Integer period,
      @JsonProperty("seasonalityType") HoltWintersModel.SeasonalityType seasonalityType,
      @JsonProperty("pad") Boolean pad,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("numPrediction") Integer numPrediction
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
        final List<Number>[] numbers = (List<Number>[]) Array.newInstance(List.class, columns.size());
        for (int i = 0; i < numericColumns.length; i++) {
          numbers[i] = Lists.newArrayList();
        }

        if (query instanceof StreamQuery) {
          Sequence<StreamQueryRow> sequence = baseRunner.run(query, responseContext);

          sequence.accumulate(
              null, new Accumulator<Object, StreamQueryRow>()
              {
                @Override
                public Object accumulate(Object accumulated, StreamQueryRow in)
                {
                  for (int i = 0; i < numericColumns.length; i++) {
                    numbers[i].add((Number) in.get(numericColumns[i]));
                  }
                  return null;
                }
              }
          );
          return hasPrediction(numbers)
                 ? Sequences.simple(Arrays.asList(makeArrayedPrediction(numericColumns, numbers)))
                 : Sequences.empty();

        } else if (query instanceof GroupByQuery) {
          final GroupByQuery groupBy = (GroupByQuery) query;
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
                        numbers[i].add((Number) in.getRaw(numericColumns[i]));
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
                      OutType output = super.accumulate(accumulated, in);
                      for (int i = 0; i < numericColumns.length; i++) {
                        numbers[i].add((Number) in.getRaw(numericColumns[i]));
                      }
                      lastTimestamp.setValue(in.getTimestampFromEpoch());
                      return output;
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
              LOG.info("Calculating %d predictions from %d rows.. ", numPrediction, numbers[0].size());
              long start = lastTimestamp.longValue();
              return hasPrediction(numbers)
                     ? Sequences.simple(makeRowedPrediction(numericColumns, numbers, start, groupBy.getGranularity()))
                     : Sequences.<Row>empty();
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

  private boolean hasPrediction(List<Number>[] numbers)
  {
    return numbers[0].size() >= period * 2;
  }

  private Map<String, Object> makeArrayedPrediction(String[] columnNames, List<Number>[] numbers)
  {
    Map<String, Object> predictions = Maps.newLinkedHashMap();
    HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    for (int i = 0; i < columnNames.length; i++) {
      predictions.put(columnNames[i], model.doPredict(numbers[i], numPrediction));
    }
    return predictions;
  }

  private List<Row> makeRowedPrediction(
      String[] columnNames,
      List<Number>[] numbers,
      long lastTimestamp,
      QueryGranularity granularity
  )
  {
    double[][] predictions = new double[columnNames.length][];
    HoltWintersModel model = new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    for (int i = 0; i < columnNames.length; i++) {
      predictions[i] = model.doPredict(numbers[i], numPrediction);
    }
    List<Row> rows = Lists.newArrayList();
    for (int p = 0; p < numPrediction; p++) {
      Map<String, Object> row = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        row.put(columnNames[i], predictions[i][p]);
      }
      rows.add(new MapBasedRow(lastTimestamp = granularity.next(lastTimestamp), row));
    }
    return rows;
  }
}
