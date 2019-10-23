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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.util.List;

/**
 */
public class HadoopDruidIndexerJob implements Jobby
{
  private static final Logger log = new Logger(HadoopDruidIndexerJob.class);
  private final HadoopDruidIndexerConfig config;
  private final MetadataStorageUpdaterJob metadataStorageUpdaterJob;
  private IndexingStatsProvider indexJob;
  private volatile List<DataSegment> publishedSegments = null;

  @Inject
  public HadoopDruidIndexerJob(
      HadoopDruidIndexerConfig config,
      MetadataStorageUpdaterJobHandler handler
  )
  {
    config.verify();
    this.config = config;

    Preconditions.checkArgument(
        !config.isUpdaterJobSpecSet() || handler != null,
        "MetadataStorageUpdaterJobHandler must not be null if ioConfig.metadataUpdateSpec is specified."
    );

    if (config.isUpdaterJobSpecSet()) {
      metadataStorageUpdaterJob = new MetadataStorageUpdaterJob(
          config,
          handler
      );
    } else {
      metadataStorageUpdaterJob = null;
    }
  }

  @Override
  public boolean run()
  {
    log.info("Running HadoopDruidIndexerJob.. %s %s", config.getDataSource(), config.getIntervals());
    List<Jobby> jobs = Lists.newArrayList();
    JobHelper.ensurePaths(config);

    switch (config.getSchema().getTuningConfig().getIngestionMode()) {
      case MAP_ONLY:
        indexJob = new MapOnlyIndexGeneratorJob(config);
        break;
      case REDUCE_MERGE:
        indexJob = new ReduceMergeIndexGeneratorJob(config);
        break;
      default:
        indexJob = new IndexGeneratorJob(config);
    }
    jobs.add(indexJob);

    if (metadataStorageUpdaterJob != null) {
      jobs.add(metadataStorageUpdaterJob);
    } else {
      log.info(
          "No metadataStorageUpdaterJob set in the config. This is cool if you are running a hadoop index task, otherwise nothing will be uploaded to database."
      );
    }

    jobs.add(
        new Jobby()
        {
          @Override
          public boolean run()
          {
            publishedSegments = IndexGeneratorJob.getPublishedSegments(config);
            return true;
          }
        }
    );


    JobHelper.runJobs(jobs, config);
    return true;
  }

  public List<DataSegment> getPublishedSegments()
  {
    if (publishedSegments == null) {
      throw new IllegalStateException("Job hasn't run yet. No segments have been published yet.");
    }
    return publishedSegments;
  }

  public IndexGeneratorStats getIndexJobStats()
  {
    return indexJob.getJobStats();
  }

  public static interface IndexingStatsProvider extends Jobby
  {
    IndexGeneratorStats getJobStats();
  }
}
