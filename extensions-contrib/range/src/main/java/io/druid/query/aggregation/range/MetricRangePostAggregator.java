package io.druid.query.aggregation.range;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class MetricRangePostAggregator implements PostAggregator
{
  private final String name;
  private final String fieldName;

  @JsonCreator
  public MetricRangePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }


  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Comparator getComparator()
  {
    return MetricRangeAggregator.COMPARATOR;
  }

  @Override
  public ValueDesc resolve(TypeResolver bindings)
  {
    return ValueDesc.DOUBLE;
  }

  @Override
  public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    return ((MetricRange) combinedAggregators.get(this.getFieldName())).getRange();
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "MetricRangePostAggregator{" +
        "fieldName='" + fieldName + '\'' +
        '}';
  }
}
