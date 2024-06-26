/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.CSVParseSpec;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularities;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class JobHelperTest
{

  public final
  @Rule
  TemporaryFolder temporaryFolder = new TemporaryFolder();

  private HadoopDruidIndexerConfig config;
  private File tmpDir;
  private File dataFile;
  private Interval interval = new Interval("2014-10-22T00:00:00Z/P1D");

  @Before
  public void setup() throws Exception
  {
    tmpDir = temporaryFolder.newFile();
    dataFile = temporaryFolder.newFile();
    config = new HadoopDruidIndexerConfig(
        new HadoopIngestionSpec(
            new DataSchema(
                "website",
                HadoopDruidIndexerConfig.JSON_MAPPER.convertValue(
                    new StringInputRowParser(
                        new CSVParseSpec(
                            new DefaultTimestampSpec("timestamp", "yyyyMMddHH", null),
                            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("host")), null, null),
                            null,
                            ImmutableList.of("timestamp", "host", "visited_num")
                        )
                    ),
                    Map.class
                ),
                new AggregatorFactory[]{new LongSumAggregatorFactory("visited_num", "visited_num")},
                new UniformGranularitySpec(
                    QueryGranularities.DAY, QueryGranularities.NONE, ImmutableList.of(this.interval)
                )
            ),
            new HadoopIOConfig(
                ImmutableMap.<String, Object>of(
                    "paths",
                    dataFile.getCanonicalPath(),
                    "type",
                    "static"
                ),
                null,
                tmpDir.getCanonicalPath()
            ),
            new HadoopTuningConfig(
                tmpDir.getCanonicalPath(),
                null,
                null,
                null,
                null,
                null,
                false,
                false,
                false,
                false,
                //Map of job properties
                ImmutableMap.of(
                    "fs.s3.impl",
                    "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
                    "fs.s3.awsAccessKeyId",
                    "THISISMYACCESSKEY"
                ),
                false,
                false,
                null,
                null,
                null
            )
        )
    );
  }

  @Test
  public void testEnsurePathsAddsProperties() throws Exception
  {
    HadoopDruidIndexerConfigSpy hadoopDruidIndexerConfigSpy = new HadoopDruidIndexerConfigSpy(config);
    JobHelper.ensurePaths(hadoopDruidIndexerConfigSpy);
    Map<String, String> jobProperties = hadoopDruidIndexerConfigSpy.getJobProperties();
    Assert.assertEquals(
        "fs.s3.impl property set correctly",
        "org.apache.hadoop.fs.s3native.NativeS3FileSystem",
        jobProperties.get("fs.s3.impl")
    );
    Assert.assertEquals(
        "fs.s3.accessKeyId property set correctly",
        "THISISMYACCESSKEY",
        jobProperties.get("fs.s3.awsAccessKeyId")
    );
  }



  private static class HadoopDruidIndexerConfigSpy extends HadoopDruidIndexerConfig
  {

    private HadoopDruidIndexerConfig delegate;
    private Map<String, String> jobProperties = new HashMap<String, String>();

    public HadoopDruidIndexerConfigSpy(HadoopDruidIndexerConfig delegate)
    {
      super(delegate.getSchema());
      this.delegate = delegate;
    }

    @Override
    public Job addInputPaths(Job job) throws IOException
    {
      Configuration configuration = job.getConfiguration();
      for (Map.Entry<String, String> en : configuration) {
        jobProperties.put(en.getKey(), en.getValue());
      }
      return job;
    }

    public Map<String, String> getJobProperties() { return jobProperties; }
  }
}
