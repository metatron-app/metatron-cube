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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.segment.serde.ColumnPartSerde;
import org.apache.lucene.index.IndexWriter;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
@JsonTypeName("lucene9")
public class Lucene9IndexingSpec extends LuceneIndexingSpec
{
  public static LuceneIndexingSpec ofAnalyzer(String textAnalyzer)
  {
    return new Lucene9IndexingSpec(textAnalyzer, null);
  }

  public static LuceneIndexingSpec of(String textAnalyzer, LuceneIndexingStrategy... strategies)
  {
    return new Lucene9IndexingSpec(textAnalyzer, Arrays.asList(strategies));
  }

  @JsonCreator
  public Lucene9IndexingSpec(
      @JsonProperty("textAnalyzer") String textAnalyzer,
      @JsonProperty("strategies") List<LuceneIndexingStrategy> strategies
  )
  {
    super(textAnalyzer, strategies);
  }

  @Override
  protected ColumnPartSerde getSerde(IndexWriter writer)
  {
    return new SerDe(writer);
  }

  @JsonTypeName("lucene9")
  public static class SerDe extends LuceneIndexingSpec.SerDe
  {
    @JsonCreator
    public SerDe() {}

    public SerDe(IndexWriter writer)
    {
      super(writer);
    }
  }
}
