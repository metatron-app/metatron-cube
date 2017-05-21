package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.yahoo.sketches.quantiles.ItemsSketch;

/**
 */
public enum SketchQuantilesOp
{
  QUANTILES {
    @Override
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter == null) {
        return sketch.getQuantiles(DEFAULT_FRACTIONS);
      } else if (parameter instanceof double[]) {
        return sketch.getQuantiles((double[])parameter);
      } else if (parameter instanceof Integer) {
        int intParam = (Integer) parameter;
        if (intParam > 0) {
          return sketch.getQuantiles(intParam); // even spaced
        }
        int limitNum = -intParam;
        double[] quantiles = new double[(int)(sketch.getN() / limitNum) + 1];
        for (int i = 1; i < quantiles.length - 1; i++) {
          quantiles[i] = (double)limitNum * i / sketch.getN();
        }
        quantiles[quantiles.length - 1] = 1;
        return sketch.getQuantiles(quantiles);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  CDF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter.getClass().isArray()) {
        return sketch.getCDF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  PMF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter.getClass().isArray()) {
        return sketch.getPMF((Object[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  IQR {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      Object[] quantiles = sketch.getQuantiles(new double[] {0.25f, 0.75f});
      if (quantiles[0] instanceof Number && quantiles[1] instanceof Number) {
        return ((Number)quantiles[1]).doubleValue() - ((Number)quantiles[0]).doubleValue();
      }
      throw new IllegalArgumentException("IQR is possible only for numeric types");
    }
  };

  public abstract Object calculate(ItemsSketch sketch, Object parameter);

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static SketchQuantilesOp fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }

  public static final double[] DEFAULT_FRACTIONS = new double[]{0d, 0.25d, 0.50d, 0.75d, 1.0d};
}
