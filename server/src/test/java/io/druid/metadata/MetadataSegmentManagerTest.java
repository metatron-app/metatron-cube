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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;


public class MetadataSegmentManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private SQLMetadataSegmentManager manager;
  private SQLMetadataSegmentPublisher publisher;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final DataSegment segment1 = new DataSegment(
      "wikipedia",
      new Interval("2012-03-15T00:00:00.000/2012-03-16T00:00:00.000"),
      "2012-03-16T00:36:30.848Z",
      ImmutableMap.<String, Object>of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=03/d=15/2012-03-16T00:36:30.848Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      NoneShardSpec.instance(),
      0,
      1234L
  );

  private final DataSegment segment2 = new DataSegment(
      "wikipedia",
      new Interval("2012-01-05T00:00:00.000/2012-01-06T00:00:00.000"),
      "2012-01-06T22:19:12.565Z",
      ImmutableMap.<String, Object>of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      NoneShardSpec.instance(),
      0,
      1234L
  );

  private final DataSegment segment3 = new DataSegment(
      "wikipedia2",
      new Interval("2012-01-05T00:00:00.000/2012-01-06T00:00:00.000"),
      "2012-01-06T22:19:12.565Z",
      ImmutableMap.<String, Object>of(
          "type", "s3_zip",
          "bucket", "test",
          "key", "wikipedia/index/y=2012/m=01/d=05/2012-01-06T22:19:12.565Z/0/index.zip"
      ),
      ImmutableList.of("dim1", "dim2", "dim3"),
      ImmutableList.of("count", "value"),
      NoneShardSpec.instance(),
      0,
      1234L
  );

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    manager = new SQLMetadataSegmentManager(
        jsonMapper,
        Suppliers.ofInstance(new MetadataSegmentManagerConfig()),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        derbyConnectorRule.getMetaDataCoordinator(),
        connector
    );

    publisher = new SQLMetadataSegmentPublisher(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector
    );

    connector.createSegmentTable();

    publisher.publishSegment(segment1);
    publisher.publishSegment(segment2);
  }

  @Test
  public void testPoll()
  {
    manager.start();
    manager.poll();
    Assert.assertEquals(Arrays.asList("wikipedia"), manager.getAllDatasourceNames());
    Assert.assertEquals(Arrays.asList(segment2, segment1), manager.getInventoryValue("wikipedia").getSegmentsSorted());
    Assert.assertNull(manager.getInventoryValue("wikipedia2"));

    manager.registerToView(segment3);
    Assert.assertEquals(Arrays.asList(segment2, segment1), manager.getInventoryValue("wikipedia").getSegmentsSorted());
    Assert.assertEquals(Arrays.asList(segment3), manager.getInventoryValue("wikipedia2").getSegmentsSorted());

    manager.poll();
    Assert.assertEquals(Arrays.asList("wikipedia"), manager.getAllDatasourceNames());
    Assert.assertEquals(Arrays.asList(segment2, segment1), manager.getInventoryValue("wikipedia").getSegmentsSorted());
    Assert.assertNull(manager.getInventoryValue("wikipedia2"));
    manager.stop();
  }

  @Test
  public void testPollWithCorruptedSegment()
  {
    //create a corrupted segment entry in segments table, which tests
    //that overall loading of segments from database continues to work
    //even in one of the entries are corrupted.
    publisher.publishSegment(
        "corrupt-segment-id",
        "corrupt-datasource",
        new DateTime(),
        "corrupt-start-date",
        "corrupt-end-date",
        true,
        "corrupt-version",
        true,
        segment1
    );

    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    manager.start();
    manager.poll();

    Assert.assertEquals(
        "wikipedia", Iterables.getOnlyElement(manager.getInventory()).getName()
    );
  }

  @Test
  public void testGetUnusedSegmentsForInterval() throws Exception
  {
    manager.start();
    manager.poll();
    Assert.assertTrue(manager.disableDatasource("wikipedia"));

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval()),
        manager.getUnusedSegmentIntervals("wikipedia", new Interval("1970/3000"), 1)
    );

    Assert.assertEquals(
        ImmutableList.of(segment2.getInterval(), segment1.getInterval()),
        manager.getUnusedSegmentIntervals("wikipedia", new Interval("1970/3000"), 5)
    );
  }
}
