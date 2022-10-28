/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.granularity.Granularity;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class Metadata
{
  // container is used for arbitrary key-value pairs in segment metadata e.g.
  // kafka firehose uses it to store commit offset
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final Map<String, Object> container;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private AggregatorFactory[] aggregators;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Granularity queryGranularity;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Granularity segmentGranularity;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer numRows;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long ingestedNumRows;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Boolean rollup;

  public Metadata()
  {
    container = new ConcurrentHashMap<>();
  }

  public Map<String, Object> getContainer()
  {
    return container;
  }

  public AggregatorFactory[] getAggregators()
  {
    return aggregators;
  }

  public Metadata setAggregators(AggregatorFactory[] aggregators)
  {
    this.aggregators = aggregators;
    return this;
  }

  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  public Metadata setQueryGranularity(Granularity queryGranularity)
  {
    this.queryGranularity = queryGranularity;
    return this;
  }

  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  public Metadata setSegmentGranularity(Granularity segmentGranularity)
  {
    this.segmentGranularity = segmentGranularity;
    return this;
  }

  public Boolean isRollup()
  {
    return rollup;
  }

  public Metadata setRollup(Boolean rollup)
  {
    this.rollup = rollup;
    return this;
  }

  public Metadata putAll(Map<String, Object> other)
  {
    if (other != null) {
      container.putAll(other);
    }
    return this;
  }

  public Object get(String key)
  {
    return container.get(key);
  }

  public Metadata put(String key, Object value)
  {
    if (value != null) {
      container.put(key, value);
    }
    return this;
  }

  public int getNumRows()
  {
    return numRows == null ? -1 : numRows;
  }

  public Metadata setNumRows(int numRows)
  {
    this.numRows = numRows;
    return this;
  }

  public long getIngestedNumRows()
  {
    return ingestedNumRows == null ? -1 : ingestedNumRows;
  }

  public Metadata setIngestedNumRow(long ingestedNumRows)
  {
    this.ingestedNumRows = ingestedNumRows;
    return this;
  }

  // arbitrary key-value pairs from the metadata just follow the semantics of last one wins if same
  // key exists in multiple input Metadata containers
  // for others e.g. Aggregators, appropriate merging is done
  public static Metadata merge(
      List<Metadata> toBeMerged,
      AggregatorFactory[] overrideMergedAggregators
  )
  {
    if (toBeMerged == null || toBeMerged.size() == 0) {
      return null;
    }

    boolean foundSomeMetadata = false;
    Map<String, Object> mergedContainer = new HashMap<>();
    List<AggregatorFactory[]> aggregatorsToMerge = overrideMergedAggregators == null
                                                   ? new ArrayList<AggregatorFactory[]>()
                                                   : null;

    List<Granularity> gransToMerge = new ArrayList<>();
    List<Granularity> segGransToMerge = new ArrayList<>();
    List<Boolean> rollupToMerge = new ArrayList<>();

    Long ingestedNumRows = 0L;
    for (Metadata metadata : toBeMerged) {
      if (metadata != null) {
        foundSomeMetadata = true;
        if (aggregatorsToMerge != null) {
          aggregatorsToMerge.add(metadata.getAggregators());
        }

        if (gransToMerge != null) {
          gransToMerge.add(metadata.getQueryGranularity());
        }
        if (segGransToMerge != null) {
          segGransToMerge.add(metadata.getSegmentGranularity());
        }
        if (ingestedNumRows == null || metadata.ingestedNumRows == null) {
          ingestedNumRows = null;
        } else {
          ingestedNumRows += metadata.ingestedNumRows;
        }

        if (rollupToMerge != null) {
          rollupToMerge.add(metadata.isRollup());
        }
        mergedContainer.putAll(metadata.container);
      } else {
        //if metadata and hence aggregators and queryGranularity for some segment being merged are unknown then
        //final merged segment should not have same in metadata
        aggregatorsToMerge = null;
        gransToMerge = null;
        segGransToMerge = null;
        rollupToMerge = null;
      }
    }

    if (!foundSomeMetadata) {
      return null;
    }

    Metadata result = new Metadata();
    if (aggregatorsToMerge != null) {
      result.setAggregators(AggregatorFactory.mergeAggregators(aggregatorsToMerge));
    } else {
      result.setAggregators(overrideMergedAggregators);
    }

    if (gransToMerge != null) {
      result.setQueryGranularity(Granularity.mergeGranularities(gransToMerge));
    }
    if (segGransToMerge != null) {
      result.setSegmentGranularity(Granularity.mergeGranularities(segGransToMerge));
    }
    if (ingestedNumRows != null) {
      result.setIngestedNumRow(ingestedNumRows);
    }

    Boolean rollup = null;
    if (rollupToMerge != null && !rollupToMerge.isEmpty()) {
      rollup = rollupToMerge.get(0);
      for (Boolean r : rollupToMerge) {
        if (r == null) {
          rollup = null;
          break;
        } else if (!r.equals(rollup)) {
          rollup = null;
          break;
        } else {
          rollup = r;
        }
      }
    }

    result.setRollup(rollup);
    result.container.putAll(mergedContainer);
    return result;

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

    Metadata metadata = (Metadata) o;

    if (!container.equals(metadata.container)) {
      return false;
    }
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(aggregators, metadata.aggregators)) {
      return false;
    }
    if (!Objects.equals(rollup, metadata.rollup)) {
      return false;
    }
    if (!Objects.equals(segmentGranularity, metadata.segmentGranularity)) {
      return false;
    }
    return Objects.equals(queryGranularity, metadata.queryGranularity);

  }

  @Override
  public int hashCode()
  {
    int result = container.hashCode();
    result = 31 * result + (aggregators != null ? Arrays.hashCode(aggregators) : 0);
    result = 31 * result + (queryGranularity != null ? queryGranularity.hashCode() : 0);
    result = 31 * result + (segmentGranularity != null ? segmentGranularity.hashCode() : 0);
    result = 31 * result + (rollup != null ? rollup.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "Metadata{" +
           "container=" + container +
           ", aggregators=" + Arrays.toString(aggregators) +
           ", queryGranularity=" + queryGranularity +
           ", segmentGranularity=" + segmentGranularity +
           ", numRows=" + numRows +
           ", ingestedNumRows=" + ingestedNumRows +
           ", rollup=" + rollup +
           '}';
  }
}
