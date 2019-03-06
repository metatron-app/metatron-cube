package io.druid.query.filter;

import io.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

public class LucenePointFilterSerDeTest
{
  @Test
  public void test() throws Exception
  {
    LucenePointFilter filter = LucenePointFilter.distance("field", 37, 120, 10000);
    String serialized = TestHelper.JSON_MAPPER.writeValueAsString(filter);
    Assert.assertEquals(
        "{\"type\":\"lucene.point\",\"field\":\"field\",\"query\":\"distance\",\"latitudes\":[37.0],\"longitudes\":[120.0],\"radiusMeters\":10000.0}",
        serialized
    );
    Assert.assertEquals(filter, TestHelper.JSON_MAPPER.readValue(serialized, DimFilter.class));
  }
}