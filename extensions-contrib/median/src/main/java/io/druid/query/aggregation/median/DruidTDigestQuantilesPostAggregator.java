package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("digestQuantiles")
public class DruidTDigestQuantilesPostAggregator implements PostAggregator
{

  private final String name;
  private final String fieldName;
  private final double[] probabilities;

  @JsonCreator
  public DruidTDigestQuantilesPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("probabilities") double[] probabilities
  )
  {
    this.name = name;
    this.fieldName = fieldName;
    double prev = 0;
    for (double probability: probabilities) {
      Preconditions.checkArgument(prev <= probability, "probabilities should be sorted");
      prev = probability;
    }
    this.probabilities = probabilities;
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
    return digest.quantiles(probabilities);
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
    return "DruidTDigestQuantilesPostAggregator{" +
        "fieldName='" + fieldName + '\'' +
        "probabilities=" + probabilities +
        '}';
  }
}