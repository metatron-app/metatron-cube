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
import com.fasterxml.jackson.annotation.JsonValue;
import com.metamx.common.ISE;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

// copy of org.elasticsearch.search.aggregations.pipeline.movavg.models.HoltWintersModel, with simplification
public class HoltWintersModel
{
  public static final String NAME = "holt_winters";

  public static final double DEFAULT_ALPHA = 0.3;
  public static final double DEFAULT_BETA = 0.1;
  public static final double DEFAULT_GAMMA = 0.3;
  public static final int DEFAULT_PERIOD = 1;
  public static final SeasonalityType DEFAULT_SEASONALITY_TYPE = SeasonalityType.ADDITIVE;
  public static final boolean DEFAULT_PAD = false;

  public double alpha()
  {
    return alpha;
  }

  public double beta()
  {
    return beta;
  }

  public double gamma()
  {
    return gamma;
  }

  public int period()
  {
    return period;
  }

  public SeasonalityType seasonalityType()
  {
    return seasonalityType;
  }

  public boolean pad()
  {
    return pad;
  }

  public enum SeasonalityType
  {
    ADDITIVE, MULTIPLICATIVE;

     @JsonValue
     @Override
     public String toString()
     {
       return name();
     }

    @JsonCreator
    public static SeasonalityType fromString(String name)
    {
      return name == null ? ADDITIVE : valueOf(name.toUpperCase());
    }
  }

  /**
   * Controls smoothing of data.  Also known as "level" value.
   * Alpha = 1 retains no memory of past values
   * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
   * mean of the series).
   */
  private final double alpha;

  /**
   * Controls smoothing of trend.
   * Beta = 1 retains no memory of past values
   * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
   * mean of the series).
   */
  private final double beta;

  /**
   * Controls smoothing of seasonality.
   * Gamma = 1 retains no memory of past values
   * (e.g. random walk), while alpha = 0 retains infinite memory of past values (e.g.
   * mean of the series).
   */
  private final double gamma;

  /**
   * Periodicity of the data
   */
  private final int period;

  /**
   * Whether this is a multiplicative or additive HW
   */
  private final SeasonalityType seasonalityType;

  /**
   * Padding is used to add a very small amount to values, so that zeroes do not interfere
   * with multiplicative seasonality math (e.g. division by zero)
   */
  private final boolean pad;
  private final double padding;

  public HoltWintersModel()
  {
    this(DEFAULT_ALPHA, DEFAULT_BETA, DEFAULT_GAMMA, DEFAULT_PERIOD, DEFAULT_SEASONALITY_TYPE, DEFAULT_PAD);
  }

  public HoltWintersModel(
      double alpha,
      double beta,
      double gamma,
      int period,
      SeasonalityType seasonalityType,
      boolean pad
  )
  {
    this.alpha = alpha;
    this.beta = beta;
    this.gamma = gamma;
    this.period = period;
    this.seasonalityType = seasonalityType;
    this.pad = pad;
    this.padding = inferPadding();
  }

  /**
   * Only pad if we are multiplicative and padding is enabled. the padding amount is not currently user-configurable.
   */
  private double inferPadding()
  {
    return seasonalityType.equals(SeasonalityType.MULTIPLICATIVE) && pad ? 0.0000000001 : 0;
  }

  /**
   * Generates a "neighboring" model, where one of the tunable parameters has been
   * randomly mutated within the allowed range.  Used for minimization
   */
  public HoltWintersModel neighboringModel()
  {
    double newValue = Math.random();
    switch ((int) (Math.random() * 3)) {
      case 0:
        return new HoltWintersModel(newValue, beta, gamma, period, seasonalityType, pad);
      case 1:
        return new HoltWintersModel(alpha, newValue, gamma, period, seasonalityType, pad);
      case 2:
        return new HoltWintersModel(alpha, beta, newValue, period, seasonalityType, pad);
      default:
        assert false : "Random value fell outside of range [0-2]";
        return new HoltWintersModel(
            newValue,
            beta,
            gamma,
            period,
            seasonalityType,
            pad
        ); // This should never technically happen...
    }
  }

  /**
   * Checks to see this model can produce a new value, without actually running the algo.
   * This can be used for models that have certain preconditions that need to be met in order
   * to short-circuit execution
   *
   * @param valuesAvailable Number of values in the current window of values
   * @return                Returns `true` if calling next() will produce a value, `false` otherwise
   */
  public boolean hasValue(int valuesAvailable)
  {
    // We need at least (period * 2) data-points (e.g. two "seasons")
    return valuesAvailable >= period * 2;
  }

  /**
   * Predicts the next `n` values in the series, using the smoothing model to generate new values.
   * Unlike the other moving averages, HoltWinters has forecasting/prediction built into the algorithm.
   * Prediction is more than simply adding the next prediction to the window and repeating.  HoltWinters
   * will extrapolate into the future by applying the trend and seasonal information to the smoothed data.
   *
   * @param values         Collection of numerics to movingAvg, usually windowed
   * @param numPredictions Number of newly generated predictions to return
   * @param <T>            Type of numeric
   *
   * @return Returns an array of doubles, since most smoothing methods operate on floating points
   */
  <T extends Number> double[] doPredict(Collection<T> values, int numPredictions)
  {
    int i = 0;
    double[] doubles = new double[values.size()];
    for (T value : values) {
      doubles[i++] = value.doubleValue();
    }
    return doPredict(doubles, numPredictions, true);
  }

  /**
   * Calculate a doubly exponential weighted moving average
   *
   * @param values       Collection of values to calculate avg for
   * @param numForecasts number of forecasts into the future to return
   *
   * @return Returns a Double containing the moving avg for the window
   */
  public double[] doPredict(final double[] values, final int numForecasts, final boolean forecastOnly)
  {
    if (values.length < period * 2) {
      // We need at least two full "seasons" to use HW
      // This should have been caught earlier, we can't do anything now...bail
      throw new ISE(
          "Holt-Winters aggregation requires at least (2 * period == 2 * "
          + period + " == " + (2 * period) + ") data-points to function.  Only [" + values.length + "] were provided."
      );
    }

    // Smoothed value
    double s = 0;
    double last_s;

    // Trend value
    double b = 0;
    double last_b;

    if (padding != 0) {
      for (int i = 0; i < values.length; i++) {
        values[i] += padding;
      }
    }

    // Initial level value is average of first season
    // Calculate the slopes between first and second season for each period
    for (int i = 0; i < period; i++) {
      s += values[i];
      b += values[i + period] - values[i];
    }
    last_s = s / period;
    last_b = b / (period * period);

    // Seasonal value
    double[] seasonal = initFirstSeason(values, new double[values.length], period);
    double[] forecasts = new double[values.length + numForecasts];

    final double _alpha = 1.0d - alpha;
    final double _beta = 1.0d - beta;
    final double _gamma = 1.0d - gamma;

    if (seasonalityType == SeasonalityType.MULTIPLICATIVE) {
      for (int i = period; i < values.length; i++) {
        forecasts[i] = last_s + last_s + seasonal[i - period];
        s = alpha * (values[i] / seasonal[i - period]) + _alpha * (last_s + last_b);
        b = beta * (s - last_s) + _beta * last_b;
        seasonal[i] = gamma * (values[i] / (last_s + last_b)) + _gamma * seasonal[i - period];

        last_s = s;
        last_b = b;
      }
    } else {
      for (int i = period; i < values.length; i++) {
        forecasts[i] = last_s + last_s + seasonal[i - period];
        s = alpha * (values[i] - seasonal[i - period]) + _alpha * (last_s + last_b);
        b = beta * (s - last_s) + _beta * last_b;
        seasonal[i] = gamma * (values[i] - (last_s - last_b)) + _gamma * seasonal[i - period];

        last_s = s;
        last_b = b;
      }
    }

    final int lastPeriod = values.length - period;
    if (seasonalityType == SeasonalityType.MULTIPLICATIVE) {
      for (int i = 0; i < numForecasts; i++) {
        int idx = lastPeriod + i % period;
        forecasts[values.length + i] = (last_s + (i + 1) * last_b) * seasonal[idx];
      }
    } else {
      for (int i = 0; i < numForecasts; i++) {
        int idx = lastPeriod + i % period;
        forecasts[values.length + i] = (last_s + (i + 1) * last_b) + seasonal[idx];
      }
    }

    return forecastOnly ? Arrays.copyOfRange(forecasts, values.length, forecasts.length) : forecasts;
  }

  private double[] initFirstSeason(double[] values, double[] seasonal, int period)
  {
    final int seasons = values.length / period;
    double[] seasonAverage = new double[seasons];
    double[] averaged = new double[values.length];

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        seasonAverage[i] += values[(i * period) + j];
      }
      seasonAverage[i] /= period;
    }

    for (int i = 0; i < seasons; i++) {
      for (int j = 0; j < period; j++) {
        averaged[(i * period) + j] = seasonAverage[i] == 0 ? 0 : values[(i * period) + j] / seasonAverage[i];
      }
    }

    for (int i = 0; i < period; i++) {
      for (int j = 0; j < seasons; j++) {
        seasonal[i] += averaged[(j * period) + i];
      }
      seasonal[i] /= seasons;
    }

    return seasonal;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(alpha, beta, gamma, period, seasonalityType, pad);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HoltWintersModel other = (HoltWintersModel) obj;
    return Objects.equals(alpha, other.alpha)
           && Objects.equals(beta, other.beta)
           && Objects.equals(gamma, other.gamma)
           && Objects.equals(period, other.period)
           && Objects.equals(seasonalityType, other.seasonalityType)
           && Objects.equals(pad, other.pad);
  }

  public HoltWintersModel withParam(double alpha, double beta, double gamma)
  {
    return new HoltWintersModelBuilder(this).alpha(alpha).beta(beta).gamma(gamma).build();
  }

  public HoltWintersModel withAlpha(double alpha)
  {
    return new HoltWintersModelBuilder(this).alpha(alpha).build();
  }

  public HoltWintersModel withBeta(double beta)
  {
    return new HoltWintersModelBuilder(this).beta(beta).build();
  }

  public HoltWintersModel withGamma(double gamma)
  {
    return new HoltWintersModelBuilder(this).gamma(gamma).build();
  }

  public HoltWintersModel withPeriod(int period)
  {
    return new HoltWintersModelBuilder(this).period(period).build();
  }

  public HoltWintersModel withPad(boolean pad)
  {
    return new HoltWintersModelBuilder(this).pad(pad).build();
  }

  public static class HoltWintersModelBuilder
  {

    private double alpha = DEFAULT_ALPHA;
    private double beta = DEFAULT_BETA;
    private double gamma = DEFAULT_GAMMA;
    private int period = DEFAULT_PERIOD;
    private SeasonalityType seasonalityType = DEFAULT_SEASONALITY_TYPE;
    private Boolean pad = null;

    public HoltWintersModelBuilder() {}

    public HoltWintersModelBuilder(HoltWintersModel model) {
      this.alpha = model.alpha;
      this.beta = model.beta;
      this.gamma = model.gamma;
      this.period = model.period;
      this.seasonalityType = model.seasonalityType;
      this.pad = model.pad;
    }

    /**
     * Alpha controls the smoothing of the data.  Alpha = 1 retains no memory of past values
     * (e.g. a random walk), while alpha = 0 retains infinite memory of past values (e.g.
     * the series mean).  Useful values are somewhere in between.  Defaults to 0.5.
     *
     * @param alpha A double between 0-1 inclusive, controls data smoothing
     *
     * @return The builder to continue chaining
     */
    public HoltWintersModelBuilder alpha(double alpha)
    {
      this.alpha = alpha;
      return this;
    }

    /**
     * Equivalent to <code>alpha</code>, but controls the smoothing of the trend instead of the data
     *
     * @param beta a double between 0-1 inclusive, controls trend smoothing
     *
     * @return The builder to continue chaining
     */
    public HoltWintersModelBuilder beta(double beta)
    {
      this.beta = beta;
      return this;
    }

    public HoltWintersModelBuilder gamma(double gamma)
    {
      this.gamma = gamma;
      return this;
    }

    public HoltWintersModelBuilder period(int period)
    {
      this.period = period;
      return this;
    }

    public HoltWintersModelBuilder seasonalityType(SeasonalityType type)
    {
      this.seasonalityType = type;
      return this;
    }

    public HoltWintersModelBuilder pad(boolean pad)
    {
      this.pad = pad;
      return this;
    }

    public HoltWintersModel build()
    {
      boolean pad = this.pad == null ? (seasonalityType == SeasonalityType.MULTIPLICATIVE) : this.pad;
      return new HoltWintersModel(alpha, beta, gamma, period, seasonalityType, pad);
    }
  }
}