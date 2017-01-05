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
      if (parameter instanceof double[]) {
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
      if (parameter instanceof String[]) {
        return sketch.getCDF((String[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
    }
  },
  PMF {
    @Override
    @SuppressWarnings("unchecked")
    public Object calculate(ItemsSketch sketch, Object parameter)
    {
      if (parameter instanceof String[]) {
        return sketch.getPMF((String[]) parameter);
      }
      throw new IllegalArgumentException("Not supported parameter " + parameter + "(" + parameter.getClass() + ")");
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
}
