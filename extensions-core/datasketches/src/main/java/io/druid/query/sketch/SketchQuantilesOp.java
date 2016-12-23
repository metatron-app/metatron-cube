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
        return sketch.getQuantiles((Integer)parameter);
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
