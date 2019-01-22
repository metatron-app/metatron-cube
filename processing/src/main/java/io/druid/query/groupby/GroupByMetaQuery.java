package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DummyQuery;
import io.druid.query.Query;
import io.druid.query.QueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

public class GroupByMetaQuery extends BaseQuery<Row> implements Query.RewritingQuery<Row>, Query.WrappingQuery<Row>
{
  private final GroupByQuery query;

  @JsonCreator
  public GroupByMetaQuery(@JsonProperty("query") GroupByQuery query)
  {
    super(query.getDataSource(), query.getQuerySegmentSpec(), query.isDescending(), query.getContext());
    this.query = query;
  }

  @JsonProperty
  public GroupByQuery getQuery()
  {
    return query;
  }

  @Override
  public String getType()
  {
    return GROUP_BY_META;
  }

  @Override
  public GroupByQuery query()
  {
    return query;
  }

  @Override
  public GroupByMetaQuery withQuery(Query query)
  {
    return new GroupByMetaQuery((GroupByQuery) query);
  }

  @Override
  public Query<Row> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new GroupByMetaQuery(query.withOverriddenContext(contextOverride));
  }

  @Override
  public Query<Row> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByMetaQuery(query.withQuerySegmentSpec(spec));
  }

  @Override
  public Query<Row> withDataSource(DataSource dataSource)
  {
    return new GroupByMetaQuery(query.withDataSource(dataSource));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Query<Row> rewriteQuery(QuerySegmentWalker segmentWalker, QueryConfig queryConfig)
  {
    try {
      Query<Row> rewritten = query.toCardinalityEstimator(queryConfig, segmentWalker.getObjectMapper(), true);
      if (rewritten instanceof RewritingQuery) {
        rewritten = ((RewritingQuery)rewritten).rewriteQuery(segmentWalker, queryConfig);
      }
      return rewritten;
    }
    catch (Exception e) {
      return new DummyQuery<Row>(
          Sequences.<Row>of(
              new MapBasedRow(0, ImmutableMap.<String, Object>of("cardinality", -1, "error", e.toString()))
          )
      );
    }
  }
}
