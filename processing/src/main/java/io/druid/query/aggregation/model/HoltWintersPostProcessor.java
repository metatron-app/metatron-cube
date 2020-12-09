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

package io.druid.query.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.Row;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.model.HoltWintersModel.SeasonalityType;
import io.druid.segment.ObjectArray;
import org.apache.commons.math3.optim.SimpleBounds;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("holtWinters")
public class HoltWintersPostProcessor extends PredictPostProcessor
{
  public static HoltWintersPostProcessor of(int numPrediction, String... columns)
  {
    return new HoltWintersPostProcessor(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        Arrays.asList(columns),
        false,
        null,
        numPrediction,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  private static final Logger LOG = new Logger(HoltWintersPostProcessor.class);

  private static final int DEFAULT_USE_LAST_N = 4096;
  private static final int DEFAULT_NUM_PREDICTION = 32;
  private static final int DEFAULT_CONFIDENCE = 95;

  private final double alpha;
  private final double beta;
  private final double gamma;
  private final int period;
  private final SeasonalityType seasonalityType;
  private final boolean pad;
  private final SimpleBounds bounds;

  @JsonCreator
  public HoltWintersPostProcessor(
      @JsonProperty("alpha") Double alpha,
      @JsonProperty("beta") Double beta,
      @JsonProperty("gamma") Double gamma,
      @JsonProperty("period") Integer period,
      @JsonProperty("seasonalityType") SeasonalityType seasonalityType,
      @JsonProperty("pad") Boolean pad,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("values") List<String> values,
      @JsonProperty("keepValuesOnly") boolean keepValuesOnly,
      @JsonProperty("useLastN") Integer useLastN,
      @JsonProperty("numPrediction") Integer numPrediction,
      @JsonProperty("confidence") Integer confidence,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregations,
      @JsonProperty("timeExpression") String timeExpression,
      @JsonProperty("timeColumn") String timeColumn,
      @JsonProperty("timeFormat") String timeFormat,
      @JsonProperty("timeLocale") String timeLocale,
      @JsonProperty("timeZone") String timeZone,
      @JsonProperty("timeGranularity") Granularity timeGranularity
  )
  {
    super(dimensions, values, keepValuesOnly, useLastN, numPrediction, confidence, postAggregations, timeExpression, timeColumn, timeFormat, timeLocale, timeZone, timeGranularity);
    this.alpha = alpha == null ? HoltWintersModel.DEFAULT_ALPHA : alpha;
    this.beta = beta == null ? HoltWintersModel.DEFAULT_BETA : beta;
    this.gamma = gamma == null ? HoltWintersModel.DEFAULT_GAMMA : gamma;
    this.period = period == null ? HoltWintersModel.DEFAULT_PERIOD : period;
    this.seasonalityType = seasonalityType == null ? HoltWintersModel.DEFAULT_SEASONALITY_TYPE : seasonalityType;
    this.pad = pad == null ? HoltWintersModel.DEFAULT_PAD : pad;
    this.bounds = new SimpleBounds(
        new double[]{alpha == null ? 0 : alpha, beta == null ? 0 : beta, gamma == null ? 0 : gamma},
        new double[]{alpha == null ? 1 : alpha, beta == null ? 1 : beta, gamma == null ? 1 : gamma}
    );
  }

  private boolean hasPrediction(int history)
  {
    return history >= period * 2;
  }

  @Override
  protected Map<String, Object> makeArrayedPrediction(String[] columnNames, BoundedTimeseries[] numbers)
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
  protected List<Row> makeRowedPrediction(
      String[] metrics,
      String[] dimensions,
      Map<ObjectArray<Object>, BoundedTimeseries[]> numbersMap,
      long lastTimestamp,
      Granularity granularity,
      List<PostAggregator.Processor> postProcessors
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
          for (PostAggregator.Processor postAggregator : postProcessors) {
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

  @JsonProperty
  public double getAlpha()
  {
    return alpha;
  }

  @JsonProperty
  public double getBeta()
  {
    return beta;
  }

  @JsonProperty
  public double getGamma()
  {
    return gamma;
  }

  @JsonProperty
  public int getPeriod()
  {
    return period;
  }

  @JsonProperty
  public SeasonalityType getSeasonalityType()
  {
    return seasonalityType;
  }

  @JsonProperty
  public boolean isPad()
  {
    return pad;
  }
}
