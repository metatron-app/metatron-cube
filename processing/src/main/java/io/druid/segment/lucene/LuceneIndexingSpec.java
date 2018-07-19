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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.data.ValueDesc;
import io.druid.segment.SecondaryIndexingSpec;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class LuceneIndexingSpec implements SecondaryIndexingSpec
{
  public static LuceneIndexingSpec ofAnalyzer(String textAnalyzer)
  {
    return new LuceneIndexingSpec(textAnalyzer, null);
  }

  public static LuceneIndexingSpec of(String textAnalyzer, LuceneIndexingStrategy... strategies)
  {
    return new LuceneIndexingSpec(textAnalyzer, Arrays.asList(strategies));
  }

  private final String textAnalyzer;
  private final List<LuceneIndexingStrategy> strategies;

  @JsonCreator
  public LuceneIndexingSpec(
      @JsonProperty("textAnalyzer") String textAnalyzer,
      @JsonProperty("strategies") List<LuceneIndexingStrategy> strategies
  )
  {
    this.textAnalyzer = textAnalyzer == null ? "standard" : textAnalyzer;
    this.strategies = strategies == null ? ImmutableList.<LuceneIndexingStrategy>of() : strategies;
  }

  @JsonProperty
  public String getTextAnalyzer()
  {
    return textAnalyzer;
  }

  @JsonProperty
  public List<LuceneIndexingStrategy> getStrategies()
  {
    return strategies;
  }

  public List<LuceneIndexingStrategy> getStrategies(String fieldName, ValueDesc valueDesc)
  {
    if (strategies.isEmpty() && valueDesc.isString()) {
      return Arrays.<LuceneIndexingStrategy>asList(new TextIndexingStrategy(fieldName));
    }
    return strategies;
  }

  public Map<String, String> getFieldDescriptors()
  {
    Map<String, String> descriptors = Maps.newLinkedHashMap();
    for (LuceneIndexingStrategy strategy : strategies) {
      String desc = strategy.getFieldDescriptor();
      if (desc != null) {
        descriptors.put(strategy.getFieldName(), desc);
      }
    }
    return descriptors;
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

    LuceneIndexingSpec that = (LuceneIndexingSpec) o;

    if (!strategies.equals(that.strategies)) {
      return false;
    }
    if (!textAnalyzer.equals(that.textAnalyzer)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = textAnalyzer.hashCode();
    result = 31 * result + strategies.hashCode();
    return result;
  }
}
