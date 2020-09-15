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

package io.druid.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.data.input.parquet.HiveParquetInputFormat;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.path.HadoopPathSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.path.PathSpecElement;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
@JsonTypeName("hive")
public class HivePathSpec implements PathSpec.Resolving
{
  private static final Logger logger = new Logger(HivePathSpec.class);

  private final String source;
  private final String metastoreUri;
  private final List<Map<String, String>> partialPartitionList;
  private final String extractPartitionRegex;
  private final String splitSize;
  private final Map<String, Object> properties;

  @JsonCreator
  public HivePathSpec(
      @JsonProperty("source") String source,
      @JsonProperty("metastoreUri") String metastoreUri,
      @JsonProperty("partialPartitions") Map<String, String> partialPartitions,
      @JsonProperty("partialPartitionList") List<Map<String, String>> partialPartitionList,
      @JsonProperty("extractPartitionRegex") String extractPartitionRegex,
      @JsonProperty("splitSize") String splitSize,
      @JsonProperty("properties") Map<String, Object> properties
  ) throws Exception
  {
    this.source = Preconditions.checkNotNull(source, "'source' cannot be null");
    this.metastoreUri = Preconditions.checkNotNull(metastoreUri, "'metastoreUri' cannot be null");
    Preconditions.checkArgument(partialPartitions == null || partialPartitionList == null);
    this.partialPartitionList = partialPartitionList == null && partialPartitions != null ?
                                Arrays.asList(partialPartitions) :
                                partialPartitionList;
    this.extractPartitionRegex = extractPartitionRegex;
    this.splitSize = splitSize;
    this.properties = properties;
  }

  @JsonProperty
  public String getSource()
  {
    return source;
  }

  @JsonProperty
  public String getMetastoreUri()
  {
    return metastoreUri;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<Map<String, String>> getPartialPartitionList()
  {
    return partialPartitionList;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getExtractPartitionRegex()
  {
    return extractPartitionRegex;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSplitSize()
  {
    return splitSize;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, Object> getProperties()
  {
    return properties;
  }

  @Override
  public PathSpec resolve()
  {
    String dbName;
    String tableName;
    int index = source.indexOf('.');
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

    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(conf);

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
      List<FieldSchema> schema = table.getCols();
      if (table.isPartitioned()) {
        schema = Lists.newArrayList(schema);
        schema.removeAll(table.getPartitionKeys());
      }

      Class inputFormat = table.getInputFormatClass();
      if (MapredParquetInputFormat.class.isAssignableFrom(inputFormat)) {
        inputFormat = HiveParquetInputFormat.class;
      }
      Set<String> pathSpecs = Sets.newTreeSet();
      if (table.isPartitioned()) {
        if (partialPartitionList == null || partialPartitionList.isEmpty()) {
          for (org.apache.hadoop.hive.metastore.api.Partition partition :
              client.listPartitions(dbName, tableName, (short) -1)) {
            pathSpecs.add(new Partition(table, partition).getLocation());
          }
        } else {
          final List<String> partitionKeys = Lists.newArrayList();
          for (FieldSchema partitionKey : table.getPartitionKeys()) {
            partitionKeys.add(partitionKey.getName());
          }
          for (Map<String, String> partialPartitionValues : partialPartitionList) {
            final List<String> partitionVals = Lists.newArrayList();
            for (String partitionKey : partitionKeys) {
              String partitionVal = partialPartitionValues.remove(partitionKey);
              if (partitionVal != null) {
                partitionVals.add(partitionVal);
              }
            }
            if (partitionVals.isEmpty()) {
              throw new IAE(
                  "cannot find any partition from %s.. partitionKeys %s",
                  partialPartitionValues, partitionKeys
              );
            }
            for (org.apache.hadoop.hive.metastore.api.Partition partition :
                client.listPartitions(dbName, tableName, partitionVals, (short) -1)) {
              pathSpecs.add(new Partition(table, partition).getLocation());
            }
          }
        }
      } else {
        if (partialPartitionList != null && !partialPartitionList.isEmpty()) {
          throw new IAE(
              "table '%s.%s' is not partitioned table.. cannot apply partition values %s",
              table.getDbName(),
              table.getTableName(),
              partialPartitionList
          );
        }
        pathSpecs.add(table.getDataLocation().toString());
      }
      logger.info("Using paths.. %s", pathSpecs);
      List<PathSpecElement> elements = Lists.newArrayList();
      for (String element : pathSpecs) {
        elements.add(new PathSpecElement(element, null, null, null));
      }
      if (elements.isEmpty()) {
        throw new IllegalArgumentException("Failed to translate hive table to path spec");
      }
      return new HadoopPathSpec(
          null,
          elements,
          inputFormat,
          -1,
          splitSize,
          false,
          table.isPartitioned(),
          extractPartitionRegex,
          properties
      );
    }
    catch (Exception ex) {
      logger.warn(ex, "Failed to translate hive table to path spec");
      throw Throwables.propagate(ex);
    }
    finally {
      if (client != null) {
        try {
          client.close();
        }
        catch (Exception e) {
          // ignore
        }
      }
    }
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    throw new IllegalStateException("should not be called directly");
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    HivePathSpec that = (HivePathSpec) o;

    if (!source.equals(that.source)) {
      return false;
    }
    if (!metastoreUri.equals(that.metastoreUri)) {
      return false;
    }
    if (!Objects.equals(partialPartitionList, that.partialPartitionList)) {
      return false;
    }
    if (!Objects.equals(extractPartitionRegex, that.extractPartitionRegex)) {
      return false;
    }
    if (!Objects.equals(properties, that.properties)) {
      return false;
    }
    if (!Objects.equals(splitSize, that.splitSize)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = source.hashCode();
    result = 31 * result + metastoreUri.hashCode();
    result = 31 * result + (partialPartitionList != null ? partialPartitionList.hashCode() : 0);
    result = 31 * result + (extractPartitionRegex != null ? extractPartitionRegex.hashCode() : 0);
    result = 31 * result + (splitSize != null ? splitSize.hashCode() : 0);
    result = 31 * result + (properties != null ? properties.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "HivePathSpec{" +
           "source='" + source + '\'' +
           ", metastoreUri='" + metastoreUri + '\'' +
           ", partialPartitionList=" + partialPartitionList +
           ", extractPartitionRegex=" + extractPartitionRegex +
           ", splitSize='" + splitSize + '\'' +
           ", properties=" + properties +
           '}';
  }
}
