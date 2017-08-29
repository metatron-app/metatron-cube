/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexIndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class IndexGeneratorJobSettlingTest
{
  private String hiveConnectorMeta = "{\"createTables\":true,\"connectURI\":\"jdbc:hive2://emn-g04-03:10000\",\"user\":\"hadoop\",\"password\":\"hadoop\"}";
  private List<String> staticColumns = Arrays.asList("module_name", "eqp_param_name");
  private List<String> regexColumns = Arrays.asList("eqp_recipe_id", "eqp_step_id", "lot_code");
  private String aggTypeColumn = "sum_type_cd";
  private String offsetColumn = "count_settling";
  private String sizeColumn = "count_activation";
  private String targetTable = "big_fdc_settling_info";
  private String condition = "count_settling > 0.0 and count_activation != -1.0";

  final private static AggregatorFactory[] aggs1 = {
      new LongSumAggregatorFactory("visited_num", "visited_num")
  };

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final String partitionType = "hashed";
  private final Interval interval = new Interval("2014-10-22T00:00:00Z/P1D");
  private final Object[][][] shardInfoForEachSegment = new Integer[][][]{{{0, 1}}};
  private final List<String> data = ImmutableList.of(
      "2014102200,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,1",
      "2014102200,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,1",
      "2014102200,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,1",
      "2014102201,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,2",
      "2014102201,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,2",
      "2014102201,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,2",
      "2014102202,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,3",
      "2014102202,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,3",
      "2014102202,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,3",
      "2014102203,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,4",
      "2014102203,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,4",
      "2014102203,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,4",
      "2014102204,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,5",
      "2014102204,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,5",
      "2014102204,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,5",
      "2014102205,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,6",
      "2014102205,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,6",
      "2014102205,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,6",
      "2014102206,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,7",
      "2014102206,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,7",
      "2014102206,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,7",
      "2014102207,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,8",
      "2014102207,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,8",
      "2014102207,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,8",
      "2014102208,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,9",
      "2014102208,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,9",
      "2014102208,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,9",
      "2014102209,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,10",
      "2014102209,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,10",
      "2014102209,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,10",
      "2014102210,TWS401_2,Spray_DIW_flow,1,AGTHM,ABC,11",
      "2014102210,TWS401_7,Spray_DIW_flow,1,AGTHM,ABC,11",
      "2014102210,TWS401_9,Spray_DIW_flow,1,AGTHM,ABC,11"
  );
  private final String inputFormatName = null;
  private final InputRowParser inputRowParser = new HadoopyStringInputRowParser(
      new CSVParseSpec(
          new DefaultTimestampSpec("timestamp", "yyyyMMddHH", null),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(
              ImmutableList.of("module_name", "eqp_param_name", "eqp_recipe_id", "eqp_step_id", "lot_code")
          ), null, null),
          null,
          ImmutableList.of("timestamp", "module_name", "eqp_param_name", "eqp_step_id", "eqp_recipe_id", "lot_code", "visited_num")
      )
  );
  private final Integer maxRowsInMemory = null;
  private final AggregatorFactory[] aggs = {
      new LongSumAggregatorFactory("visited_num", "visited_num")
  };
  private final String datasourceName = "website";

  private final String settlingYNColumnName = "settling";

  private ObjectMapper mapper;
  private HadoopDruidIndexerConfig config;
  private File dataFile;
  private File tmpDir;

  public IndexGeneratorJobSettlingTest()
  {
  }

  private void writeDataToLocalSequenceFile(File outputFile, List<String> data) throws IOException
  {
    Configuration conf = new Configuration();
    LocalFileSystem fs = FileSystem.getLocal(conf);
    Writer fileWriter = SequenceFile.createWriter(
        fs,
        conf,
        new Path(outputFile.getAbsolutePath()),
        BytesWritable.class,
        BytesWritable.class,
        SequenceFile.CompressionType.NONE,
        (CompressionCodec) null
    );

    int keyCount = 10;
    for (String line : data) {
      ByteBuffer buf = ByteBuffer.allocate(4);
      buf.putInt(keyCount);
      BytesWritable key = new BytesWritable(buf.array());
      BytesWritable value = new BytesWritable(line.getBytes(Charsets.UTF_8));
      fileWriter.append(key, value);
      keyCount += 1;
    }

    fileWriter.close();
  }

  @Before
  public void setUp() throws Exception
  {
    mapper = HadoopDruidIndexerConfig.JSON_MAPPER;
    mapper.registerSubtypes(new NamedType(HashBasedNumberedShardSpec.class, "hashed"));
    mapper.registerSubtypes(new NamedType(SingleDimensionShardSpec.class, "single"));

    dataFile = temporaryFolder.newFile();
    tmpDir = temporaryFolder.newFolder();

    HashMap<String, Object> inputSpec = new HashMap<String, Object>();
    inputSpec.put("paths", dataFile.getCanonicalPath());
    inputSpec.put("type", "static");
    if (inputFormatName != null) {
      inputSpec.put("inputFormat", inputFormatName);
    }

    if (SequenceFileInputFormat.class.getName().equals(inputFormatName)) {
      writeDataToLocalSequenceFile(dataFile, data);
    } else {
      FileUtils.writeLines(dataFile, data);
    }

    List<String> columns = ListUtils.union(staticColumns, regexColumns);
    columns.add(aggTypeColumn);
    columns.add(offsetColumn);
    columns.add(sizeColumn);

    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                datasourceName,
                mapper.convertValue(
                    inputRowParser,
                    Map.class
                ),
                aggs,
                new UniformGranularitySpec(
                    Granularity.DAY, QueryGranularities.DAY, ImmutableList.of(this.interval)
                ),
                mapper
            ),
            new HadoopIOConfig(
                ImmutableMap.copyOf(inputSpec),
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
                null,
                null,
                maxRowsInMemory,
                false,
                false,
                false,
                false,
                ImmutableMap.of(JobContext.NUM_REDUCES, "0"), //verifies that set num reducers is ignored
                false,
                false,
                null,
                true,
                null
            ),
            null,
            new HadoopJDBCSettlingConfig(
                mapper.readValue(hiveConnectorMeta, MetadataStorageConnectorConfig.class),
                String.format("select %s from %s where %s", StringUtils.join(columns, ","), targetTable, condition),
                staticColumns,
                regexColumns,
                "eqp_param_name",
                "visited_num",
                aggTypeColumn,
                offsetColumn,
                sizeColumn,
                settlingYNColumnName
            )
        )
    );

    config.setShardSpecs(loadShardSpecs(partitionType, shardInfoForEachSegment));
    config = HadoopDruidIndexerConfig.fromSpec(config.getSchema());
  }

  private List<ShardSpec> constructShardSpecFromShardInfo(String partitionType, Object[][] shardInfoForEachShard)
  {
    List<ShardSpec> specs = Lists.newArrayList();
    if (partitionType.equals("hashed")) {
      for (Integer[] shardInfo : (Integer[][]) shardInfoForEachShard) {
        specs.add(new HashBasedNumberedShardSpec(shardInfo[0], shardInfo[1], null, HadoopDruidIndexerConfig.JSON_MAPPER));
      }
    } else if (partitionType.equals("single")) {
      int partitionNum = 0;
      for (String[] shardInfo : (String[][]) shardInfoForEachShard) {
        specs.add(new SingleDimensionShardSpec("host", shardInfo[0], shardInfo[1], partitionNum++));
      }
    } else {
      throw new RuntimeException(String.format("Invalid partition type:[%s]", partitionType));
    }

    return specs;
  }

  private Map<DateTime, List<HadoopyShardSpec>> loadShardSpecs(
      String partitionType,
      Object[][][] shardInfoForEachShard
  )
  {
    Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
    int shardCount = 0;
    int segmentNum = 0;
    for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
      List<ShardSpec> specs = constructShardSpecFromShardInfo(partitionType, shardInfoForEachShard[segmentNum++]);
      List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
      for (int i = 0; i < specs.size(); ++i) {
        actualSpecs.add(new HadoopyShardSpec(specs.get(i), shardCount++));
      }

      shardSpecs.put(segmentGranularity.getStart(), actualSpecs);
    }

    return shardSpecs;
  }

  @Test
  public void testIndexGeneratorJob() throws IOException
  {
    verifyJob(new IndexGeneratorJob(config));
  }

  private void verifyJob(IndexGeneratorJob job) throws IOException
  {
    JobHelper.runJobs(ImmutableList.<Jobby>of(job), config);

    int segmentNum = 0;
    for (DateTime currTime = interval.getStart(); currTime.isBefore(interval.getEnd()); currTime = currTime.plusDays(1)) {
      Object[][] shardInfo = shardInfoForEachSegment[segmentNum++];
      File segmentOutputFolder = new File(
          String.format(
              "%s/%s/%s_%s/%s",
              config.getSchema().getIOConfig().getSegmentOutputPath(),
              config.getSchema().getDataSchema().getDataSource(),
              currTime.toString(),
              currTime.plusDays(1).toString(),
              config.getSchema().getTuningConfig().getVersion()
          )
      );
      Assert.assertTrue(segmentOutputFolder.exists());
      Assert.assertEquals(shardInfo.length, segmentOutputFolder.list().length);

      for (int partitionNum = 0; partitionNum < shardInfo.length; ++partitionNum) {
        File individualSegmentFolder = new File(segmentOutputFolder, Integer.toString(partitionNum));
        Assert.assertTrue(individualSegmentFolder.exists());

        File descriptor = new File(individualSegmentFolder, "descriptor.json");
        File indexZip = new File(individualSegmentFolder, "index.zip");
        Assert.assertTrue(descriptor.exists());
        Assert.assertTrue(indexZip.exists());

        DataSegment dataSegment = mapper.readValue(descriptor, DataSegment.class);
        Assert.assertEquals(config.getSchema().getTuningConfig().getVersion(), dataSegment.getVersion());
        Assert.assertEquals(new Interval(currTime, currTime.plusDays(1)), dataSegment.getInterval());
        Assert.assertEquals("local", dataSegment.getLoadSpec().get("type"));
        Assert.assertEquals(indexZip.getCanonicalPath(), dataSegment.getLoadSpec().get("path"));
        Assert.assertEquals(Integer.valueOf(9), dataSegment.getBinaryVersion());

        Assert.assertEquals(datasourceName, dataSegment.getDataSource());
        Assert.assertEquals("module_name", dataSegment.getDimensions().get(0));
        Assert.assertEquals("eqp_param_name", dataSegment.getDimensions().get(1));
        Assert.assertEquals("eqp_recipe_id", dataSegment.getDimensions().get(2));
        Assert.assertEquals("eqp_step_id", dataSegment.getDimensions().get(3));
        Assert.assertEquals("lot_code", dataSegment.getDimensions().get(4));
        Assert.assertEquals("visited_num", dataSegment.getMetrics().get(0));

        Integer[] hashShardInfo = (Integer[]) shardInfo[partitionNum];
        HashBasedNumberedShardSpec spec = (HashBasedNumberedShardSpec) dataSegment.getShardSpec();
        Assert.assertEquals((int) hashShardInfo[0], spec.getPartitionNum());
        Assert.assertEquals((int) hashShardInfo[1], spec.getPartitions());

        File dir = Files.createTempDir();

        unzip(indexZip, dir);

        QueryableIndex index = HadoopDruidIndexerConfig.INDEX_IO.loadIndex(dir);
        QueryableIndexIndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);

        Assert.assertTrue(index.getNumRows() == 3);

        int count60 = 0;
        int count66 = 0;
        for (Rowboat row : adapter.getRows(1))
        {
          Long sum = (Long)row.getMetrics()[0];
          if (sum == 60) {
            count60++;
          } else if (sum == 66) {
            count66++;
          }
        }

        Assert.assertTrue(count60 == 2);
        Assert.assertTrue(count66 == 1);
      }
    }
  }

  private void unzip(File zip, File outDir)
  {
    try {
      long size = 0L;
      final byte[] buffer = new byte[1 << 13];
      try (ZipInputStream in = new ZipInputStream(new FileInputStream(zip))) {
        for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
          final String fileName = entry.getName();
          try (final OutputStream out = new BufferedOutputStream(
              new FileOutputStream(
                  outDir.getAbsolutePath()
                      + File.separator
                      + fileName
              ), 1 << 13
          )) {
            for (int len = in.read(buffer); len >= 0; len = in.read(buffer)) {
              if (len == 0) {
                continue;
              }
              size += len;
              out.write(buffer, 0, len);
            }
            out.flush();
          }
        }
      }
    }
    catch (IOException | RuntimeException exception) {
    }
  }

}
