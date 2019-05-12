package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.query.filter.LucenePointFilter;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.select.Schema;
import io.druid.query.select.SchemaQuery;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestEstateQuery extends QueryRunnerTestHelper
{
  @Test
  public void testSchema()
  {
    Schema schema = (Schema) Iterables.getOnlyElement(runQuery(SchemaQuery.of("estate")));
    Assert.assertEquals(Arrays.asList("__time", "idx", "gu"), schema.getDimensionNames());
    Assert.assertEquals(Arrays.asList("gis", "amt", "py"), schema.getMetricNames());
    Assert.assertEquals(
        "[long, dimension.string, dimension.string, struct(lat:double,lon:double,addr:string), long, float]",
        schema.getColumnTypes().toString()
    );
    Assert.assertEquals(
        "{gis={coord=point(latitude=lat,longitude=lon), addr=text}}", schema.getDescriptors().toString()
    );
  }

  @Test
  public void testPointFilter()
  {
    String[] columns = new String[]{"gu", "amt", "py", "gis.addr"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("estate")
        .columns(columns)
        .orderBy(OrderByColumnSpec.descending("amt"))
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "toMap", "timestampColumn", "__time"));


    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
    );
    builder.filters(LucenePointFilter.distance("gis.coord", 37.492927, 127.020779, 400));
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    expected = createExpectedMaps(
        columns,
        new Object[]{"서초구", 88500L, 114.84F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 82700L, 84.82F, "서초동 1687 유원서초"},
        new Object[]{"서초구", 81000L, 84.82F, "서초동 1687 유원서초"},
        new Object[]{"서초구", 74800L, 84.78F, "서초동 1648-2 서초현대"},
        new Object[]{"서초구", 70000L, 84.04F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 68500L, 83.25F, "서초동 1644 현대서초3"},
        new Object[]{"서초구", 67000L, 84.49F, "서초동 1644 현대서초3"}
    );
    builder.filters(LucenePointFilter.nearest("gis.coord", 37.492927, 127.020779, 7));
    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }
}
