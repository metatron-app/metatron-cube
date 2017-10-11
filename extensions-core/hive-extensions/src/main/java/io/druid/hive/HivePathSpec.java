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

package io.druid.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.indexer.path.HynixPathSpec;
import io.druid.indexer.path.HynixPathSpecElement;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
@JsonTypeName("hive")
public class HivePathSpec extends HynixPathSpec
{
  private static final Logger logger = new Logger(HivePathSpec.class);

  @JsonCreator
  public HivePathSpec(
      @JsonProperty("source") String source,
      @JsonProperty("metastoreUri") String metastoreUri,
      @JsonProperty("partialPartitions") Map<String, String> partialPartitions,
      @JsonProperty("partialPartitionList") List<Map<String, String>> partialPartitionList,
      @JsonProperty("splitSize") String splitSize,
      @JsonProperty("properties") Map<String, Object> properties
  ) throws Exception
  {
    super(extract(source, metastoreUri, partialPartitions, partialPartitionList, splitSize, properties));
  }

  private static HynixPathSpec extract(
      String source,
      String metastoreUri,
      Map<String, String> partialPartitions,
      List<Map<String, String>> partialPartitionList,
      String splitSize,
      Map<String, Object> properties
  )
      throws Exception
  {
    Preconditions.checkArgument(partialPartitions == null || partialPartitionList == null);
    if (partialPartitionList == null && partialPartitions != null) {
      partialPartitionList = Arrays.asList(partialPartitions);
    }
    String dbName;
    String tableName;
    int index = Preconditions.checkNotNull(source, "source cannot be null").indexOf('.');
    if (index < 0) {
      dbName = "default";
      tableName = source;
    } else {
      dbName = source.substring(0, index);
      tableName = source.substring(index + 1);
    }

    HiveConf conf = new HiveConf();
    if (metastoreUri != null && !metastoreUri.isEmpty()) {
      conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
    }
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);

    try {
      Table table = new Table(client.getTable(dbName, tableName));
      logger.info(
          "Got table '%s.%s'. partitioned=%s, format=%s, serde=%s, location=%s",
          table.getDbName(),
          table.getTableName(),
          table.isPartitioned(),
          table.getInputFormatClass(),
          table.getSerializationLib(),
          table.getDataLocation()
      );
      // todo rewrite parser spec with serde

      Class inputFormat = table.getInputFormatClass();
      Set<String> pathSpecs = Sets.newTreeSet();
      if (table.isPartitioned()) {
        if (partialPartitionList == null || partialPartitionList.isEmpty()) {
          for (org.apache.hadoop.hive.metastore.api.Partition partition :
              client.listPartitions(dbName, tableName, (short)-1)) {
            pathSpecs.add(new Partition(table, partition).getLocation());
          }
        } else {
          for (Map<String, String> partialPartitionValues : partialPartitionList) {
            List<String> partitionVals = Lists.newArrayList();
            for (FieldSchema partitionKey : table.getPartitionKeys()) {
              String partitionVal = partialPartitionValues.remove(partitionKey.getName());
              if (partitionVal != null) {
                partitionVals.add(partitionVal);
                continue;
              }
              if (!partialPartitionValues.isEmpty()) {
                logger.warn("some partition values are not used.. %s" + partialPartitionList);
              }
              break;
            }
            for (org.apache.hadoop.hive.metastore.api.Partition partition :
                partitionVals.isEmpty() ?
                client.listPartitions(dbName, tableName, (short) -1) :
                client.listPartitions(dbName, tableName, partitionVals, (short) -1)) {
              pathSpecs.add(new Partition(table, partition).getLocation());
            }
          }
        }
      } else {
        if (partialPartitionList != null && !partialPartitionList.isEmpty()) {
          logger.warn(
              "table '%s.%s' is not partitioned table.. ignoring partition values %s",
              table.getDbName(),
              table.getTableName(),
              partialPartitionList
          );
        }
        pathSpecs.add(table.getDataLocation().toString());
      }
      logger.info("Using paths.. ", pathSpecs);
      List<HynixPathSpecElement> elements = Lists.newArrayList();
      for (String element : pathSpecs) {
        elements.add(new HynixPathSpecElement(element, null, null, null));
      }
      return new HynixPathSpec(
          null,
          elements,
          inputFormat,
          splitSize,
          false,
          table.isPartitioned(),
          properties
      );
    }
    catch (Exception ex) {
      logger.warn(ex, "Failed to translate hive table to path spec");
      throw Throwables.propagate(ex);
    }
    finally {
      client.close();
    }
  }
}
