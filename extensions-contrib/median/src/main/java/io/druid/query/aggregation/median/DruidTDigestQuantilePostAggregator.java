package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("digestQuantile")
public class DruidTDigestQuantilePostAggregator implements PostAggregator
{

  private final String name;
  private final String fieldName;
  private final double probability;

  @JsonCreator
  public DruidTDigestQuantilePostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probability") double probability
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    this.probability = probability;
  }

  @Override
  public Comparator getComparator()
  {
    return DruidTDigestAggregator.COMPARATOR;
  }

  @Override
  public Set<String> getDependentFields()
  {
    return Sets.newHashSet(fieldName);
  }

  @Override
  public Object compute(Map<String, Object> values)
  {
    final DruidTDigest digest = (DruidTDigest) values.get(this.getFieldName());
    return digest.quantile(probability);
  }

  @Override
  public String getName()
  {
    return name;
  }

  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public String toString()
  {
    return "DruidTDigestQuantilePostAggregator{" +
        "fieldName='" + fieldName + '\'' +
        "probability=" + probability +
        '}';
  }
}