package io.druid.query.aggregation.range;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.AggregationTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class MetricRangeAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ObjectMapper mapper;

  public MetricRangeAggregationTest()
  {
    MetricRangeModule mm = new MetricRangeModule();
    mm.configure(null);

    mapper = new DefaultObjectMapper();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(mm.getJacksonModules(), temporaryFolder);
  }

  @Test
  public void testSimpleDataIngetsionAndGroupByTest() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        QueryGranularities.DAY,
        100,
        readFileFromClasspathAsString("simple_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(15, results.size());
  }

  @Test
  public void testSerde() throws Exception {
    MetricRangeAggregatorFactory factory = new MetricRangeAggregatorFactory("name", "fieldName");

    String factoryString = mapper.writeValueAsString(factory);
    MetricRangeAggregatorFactory factory2 = mapper.readValue(factoryString, MetricRangeAggregatorFactory.class);

    Assert.assertEquals(factory.getName(), factory2.getName());
    Assert.assertEquals(factory.requiredFields(), factory2.requiredFields());
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(MetricRangeAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
