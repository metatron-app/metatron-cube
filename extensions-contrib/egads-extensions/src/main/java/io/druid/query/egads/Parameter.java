package io.druid.query.egads;

/**
 */
public class Parameter
{
  public static Parameter of(String name, Class type)
  {
    return new Parameter(name, type, false, null);
  }

  public static Parameter optional(String name, Class type, Object defaultValue)
  {
    return new Parameter(name, type, true, defaultValue);
  }

  private final String name;
  private final Class type;
  private final boolean optional;
  private final Object defaultValue;

  private Parameter(String name, Class type, boolean optional, Object defaultValue)
  {
    this.name = name;
    this.type = type;
    this.optional = optional;
    this.defaultValue = defaultValue;
  }

  public String getName()
  {
    return name;
  }

  public Class getType()
  {
    return type;
  }

  public boolean isOptional()
  {
    return optional;
  }

  public Object getDefaultValue()
  {
    return defaultValue;
  }
}
