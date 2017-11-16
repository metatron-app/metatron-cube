package io.druid.query.aggregation.median;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("digestMedian")
public class DruidTDigestMedianPostAggregator implements PostAggregator
{

  private String name;
  private String fieldName;

  @JsonCreator
  public DruidTDigestMedianPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
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
  public Object compute(DateTime timestamp, Map<String, Object> values)
  {
    final DruidTDigest digest = (DruidTDigest) values.get(this.getFieldName());
    return digest.median();
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
    return "DruidTDigestMedianPostAggregator{" +
        "fieldName='" + fieldName + '\'' +
        '}';
  }
}