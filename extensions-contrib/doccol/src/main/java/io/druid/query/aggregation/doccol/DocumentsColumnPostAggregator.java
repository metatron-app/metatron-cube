package io.druid.query.aggregation.doccol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Sets;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

@JsonTypeName("docMerge")
public class DocumentsColumnPostAggregator implements PostAggregator
{
  private static final Comparator COMPARATOR = DocumentsColumnAggregator.COMPARATOR;

  private final String name;
  private final String fieldName;

  @JsonCreator
  public DocumentsColumnPostAggregator(
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
    return COMPARATOR;
  }

  @Override
  public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
  {
    return ((DocumentsColumn) combinedAggregators.get(this.getFieldName())).get();
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
    return "DocumentsColumnPostAggregator{" +
           "fieldName='" + fieldName + '\'' +
           '}';
  }
}
