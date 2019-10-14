package io.druid.query.aggregation.countmin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class CountMinPostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;
  private final String value;

  @JsonCreator
  public CountMinPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("value") String value
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.value = value;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return ImmutableSet.of(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return GuavaUtils.NULL_FIRST_NATURAL;
  }

  @Override
  public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    CountMinSketch sketch = (CountMinSketch) combinedAggregators.get(fieldName);
    return sketch == null ? null : sketch.getEstimatedCount(value.getBytes());
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.LONG;
  }
}
