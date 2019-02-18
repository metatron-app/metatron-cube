package io.druid.segment.serde;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableMap;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.segment.lucene.LuceneExtensionModule;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.segment.lucene.SpatialOperations;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestShapeQuery extends QueryRunnerTestHelper
{
  static {
    for (Module jacksonModule : new LuceneExtensionModule().getJacksonModules()) {
      TestHelper.JSON_MAPPER.registerModule(jacksonModule);
    }
    TestIndex.addIndex("seoul_roads", "seoul_roads_schema.json", "seoul_roads.tsv");
  }

  @Test
  public void testSimpleFilter()
  {
    String[] columns = new String[]{"name", "geom"};
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("seoul_roads")
        .columns(columns)
        .addContext(Query.POST_PROCESSING, ImmutableMap.of("type", "tabular", "timestampColumn", "__time"));

    List<Map<String, Object>> expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)"},
        new Object[]{"테헤란로", "LINESTRING (127.027648 37.497879, 127.066436 37.509842)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));

    builder.filters(new LuceneSpatialFilter(
        "geom",
        SpatialOperations.INTERSECTS,
        ShapeFormat.WKT,
        "POLYGON ((127.011136 37.494466, 127.024620 37.494036, 127.026753 37.502427, 127.011136 37.494466))"
    ));
    expected = createExpectedMaps(
        columns,
        new Object[]{"강남대로", "LINESTRING (127.034182 37.484505, 127.021399 37.511051, 127.017827 37.521752)"},
        new Object[]{"서초대로", "LINESTRING (127.007656 37.491764, 127.027648 37.497879)"}
    );
    Assert.assertEquals(expected, runQuery(builder.streaming()));
  }
}
