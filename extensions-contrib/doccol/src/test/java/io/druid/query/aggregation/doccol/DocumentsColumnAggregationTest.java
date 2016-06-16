package io.druid.query.aggregation.doccol;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregationTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class DocumentsColumnAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public DocumentsColumnAggregationTest()
  {
    DocumentsColumnModule dm = new DocumentsColumnModule();
    dm.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(dm.getJacksonModules(), tempFolder);
  }

  @Test
  public void testSimpleDataIngestAndGroupByTest() throws Exception {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        100,
        readFileFromClasspathAsString("simple_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(15, results.size());
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(DocumentsColumnAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
