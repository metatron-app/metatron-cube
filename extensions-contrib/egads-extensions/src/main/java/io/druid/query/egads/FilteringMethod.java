package io.druid.query.egads;

/**
 */
public enum FilteringMethod
{
  GAP_RATIO(0.01),
  EIGEN_RATIO(0.1),
  EXPLICIT(10),
  K_GAP(8),
  VARIANCE(0.99),
  SMOOTHNESS(0.97);

  private final double param;

  FilteringMethod(double param)
  {
    this.param = param;
  }

  public double param()
  {
    return param;
  }
}
