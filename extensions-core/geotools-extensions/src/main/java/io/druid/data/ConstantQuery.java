package io.druid.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import io.druid.common.utils.Sequences;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("constant")
@SuppressWarnings("unchecked")
public class ConstantQuery extends BaseQuery implements Query.ArrayOutputSupport
{
  private final List<String> colunmNames;
  private final List<Object[]> values;

  @JsonCreator
  public ConstantQuery(
      @JsonProperty("colunmNames") List<String> colunmNames,
      @JsonProperty("values") List<Object[]> values
  )
  {
    this(TableDataSource.of("<NOT-EXISTING>"), null, colunmNames, values, Maps.<String, Object>newHashMap());
  }

  private ConstantQuery(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      List<String> colunmNames,
      List<Object[]> values,
      Map<String, Object> context
  )
  {
    super(dataSource, querySegmentSpec, false, context);
    this.colunmNames = colunmNames;
    this.values = values;
  }

  @Override
  public String getType()
  {
    return "constant";
  }

  @JsonProperty
  public List<String> getColunmNames()
  {
    return colunmNames;
  }

  @JsonProperty
  public List<Object[]> getValues()
  {
    return values;
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new ConstantQuery(
        getDataSource(),
        spec,
        colunmNames,
        values,
        getContext()
    );
  }

  @Override
  public Query withDataSource(DataSource dataSource)
  {
    return new ConstantQuery(
        dataSource,
        getQuerySegmentSpec(),
        colunmNames,
        values,
        getContext()
    );
  }

  @Override
  public Query withOverriddenContext(Map contextOverride)
  {
    return new ConstantQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        colunmNames,
        values,
        computeOverriddenContext(contextOverride)
    );
  }

  @Override
  public List<String> estimatedOutputColumns()
  {
    return colunmNames;
  }

  @Override
  public Sequence<Object[]> array(Sequence sequence)
  {
    return Sequences.simple(values);
  }

  @Override
  public boolean equals(Object o)
  {
    ConstantQuery other = (ConstantQuery) o;
    if (!colunmNames.equals(other.colunmNames)) {
      return false;
    }
    if (values.size() != other.values.size()) {
      return false;
    }
    for (int i = 0; i < values.size(); i++) {
      if (!Arrays.equals(values.get(i), other.values.get(i))) {
        return false;
      }
    }
    return true;
  }
}
