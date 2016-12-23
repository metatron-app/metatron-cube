package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 */
public enum SketchOp
{
  THETA {
    @Override
    public SketchHandler handler()
    {
      return new SketchHandler.Theta();
    }
  },
  QUANTILE {
    @Override
    public SketchHandler handler()
    {
      return new SketchHandler.Quantile();
    }
  };

  public abstract SketchHandler handler();

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static SketchOp fromString(String name)
  {
    return name == null ? null : valueOf(name.toUpperCase());
  }
}
