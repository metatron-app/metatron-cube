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

import com.metamx.common.logger.Logger;
import org.apache.commons.math3.analysis.MultivariateFunction;
import org.apache.commons.math3.optim.InitialGuess;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.SimpleBounds;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.nonlinear.scalar.ObjectiveFunction;
import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.BOBYQAOptimizer;

import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class Predictions
{
  private static final Logger LOG = new Logger(Predictions.class);

  public static double getCIMultiplierByPerc(int percentage)
  {
    switch (percentage) {
      case 50: return 2.58; //50+50
      case 55: return 1.96; //45+50
      case 60: return 1.64;
      case 65: return 1.44;
      case 70: return 1.28;
      case 75: return 1.15;
      case 80: return 1.04;
      case 85: return 0.93;
      case 90: return 0.84;
      case 95: return 0.76; //5+50
      case 99: return 0.67; //1+50
      default:
        throw new IllegalArgumentException("Invalid percentage " + percentage);
    }
  }

  public static double[][] predict(
      HoltWintersModel model,
      final double[] tsData,
      int numPrediction,
      int ci,
      boolean predictOnly
  )
  {
    HoltWintersModel optimized = optimize(model, tsData);

    LOG.info(
        "predicting %d from %d values with param (%f, %f, %f), %d period",
        numPrediction,
        tsData.length,
        optimized.alpha(),
        optimized.beta(),
        optimized.gamma(),
        optimized.period()
    );
    final double[] predictions = optimized.doPredict(tsData, numPrediction, false);   // tsData.length + numForecasts

    double variance = 0;
    for (int i = optimized.period(); i < tsData.length; i++) {
      variance += Math.pow(predictions[i] - tsData[i], 2);
    }
    final double stddev = Math.sqrt(variance / (tsData.length - optimized.period()));
    final double interval = getCIMultiplierByPerc(ci) * stddev;
    LOG.info("-- variance %f, stddev %f, interval %f", variance, stddev, interval);

    if (predictOnly) {
      final double[][] ranged = new double[numPrediction][3];
      for (int i = 0; i < numPrediction; i++) {
        final double predict = interval * Math.log(i + 2);
        ranged[i][0] = predictions[tsData.length + i] - predict;
        ranged[i][1] = predictions[tsData.length + i];
        ranged[i][2] = predictions[tsData.length + i] + predict;
      }
      return ranged;
    }
    final double[][] ranged = new double[predictions.length][3];
    for (int i = optimized.period(); i < tsData.length; i++) {
        ranged[i][0] = predictions[tsData.length + i] - interval;
        ranged[i][1] = predictions[tsData.length + i];
        ranged[i][2] = predictions[tsData.length + i] + interval;
    }
    for (int i = tsData.length; i < predictions.length; i++) {
      final double predict = interval * Math.log(i + 2);
        ranged[i][0] = predictions[tsData.length + i] - predict;
        ranged[i][1] = predictions[tsData.length + i];
        ranged[i][2] = predictions[tsData.length + i] + predict;
    }
    return ranged;
  }

  private static HoltWintersModel optimize(final HoltWintersModel model, final double[] tsData)
  {
    final AtomicReference<double[]> bestTillNow = new AtomicReference<>();
    ObjectiveFunction function = new ObjectiveFunction(
        new MultivariateFunction()
        {
          @Override
          public double value(double[] point)
          {
            double value = sse(model.withAlpha(point[0]).withBeta(point[1]).withGamma(point[2]), tsData);
            if (bestTillNow.get() == null || bestTillNow.get()[0] > value) {
              bestTillNow.set(new double[] {value, point[0], point[1], point[2]});
            }
            return value;
          }
        }
    );

    BOBYQAOptimizer optimizer = new BOBYQAOptimizer(7);
    InitialGuess initGuess = new InitialGuess(new double[]{0.3, 0.1, 0.1});
    MaxIter maxIter = new MaxIter(30000);
    MaxEval maxEval = new MaxEval(30000);
    GoalType goal = GoalType.MINIMIZE;
    SimpleBounds bounds = new SimpleBounds(new double[]{0.0, 0.0, 0.0}, new double[]{1.0, 1.0, 1.0});
    try {
      PointValuePair optimal = optimizer.optimize(function, goal, bounds, initGuess, maxIter, maxEval);
      double[] params = optimal.getPoint();

      return model.withAlpha(params[0]).withBeta(params[1]).withGamma(params[2]);
    }
    catch (Exception e) {
      LOG.warn("Failed to optimize.. %s", e.toString());
      double[] point = bestTillNow.get();
      if (point == null) {
        return model;
      }
      return model.withAlpha(point[1]).withBeta(point[2]).withGamma(point[3]);
    }
  }

  public static double sse(HoltWintersModel model, double[] values)
  {
    double[] evaluation = model.doPredict(values, 0, false);

    double sqrErrors = 0.0;
    for (int i = model.period(); i < values.length; i++) {
      double error = values[i] - evaluation[i];
      sqrErrors += error * error;
    }
    return sqrErrors;
  }
}
