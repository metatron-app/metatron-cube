package io.druid.query.egads;

import com.yahoo.egads.models.tsmm.TimeSeriesModel;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 */
public enum TsModel
{
  DoubleExponentialSmoothingModel,
  MovingAverageModel,
  MultipleLinearRegressionModel,
  NaiveForecastingModel,
  OlympicModel {
    @Override
    public List<Parameter> parameters()
    {
      return Arrays.asList(
          Parameter.of("TIME_SHIFTS", int[].class),
          Parameter.of("BASE_WINDOWS", int[].class),
          Parameter.of("NUM_WEEKS", int.class),
          Parameter.of("NUM_TO_DROP", int.class),
          Parameter.optional("DYNAMIC_PARAMETERS", int.class, null)
      );
    }
  },
  PolynomialRegressionModel,
  RegressionModel,
  SimpleExponentialSmoothingModel,
  TripleExponentialSmoothingModel,
  WeightedMovingAverageModel,
  SpectralSmootherModel {
    @Override
    public List<Parameter> parameters()
    {
      return Arrays.asList(
          Parameter.of("WINDOW_SIZE", int.class),
          Parameter.of("FILTERING_METHOD", FilteringMethod.class),
          Parameter.optional("FILTERING_PARAM", double.class, null)
      );
    }
  },
  Null;

  public List<Parameter> parameters()
  {
    return Arrays.asList();
  }

  public TimeSeriesModel newInstance(Properties properties)
  {
    return Utils.newInstance("com.yahoo.egads.models.tsmm." + name(), TimeSeriesModel.class, properties);
  }
}
