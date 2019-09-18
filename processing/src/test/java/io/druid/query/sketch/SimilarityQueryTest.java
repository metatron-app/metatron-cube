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

package io.druid.query.sketch;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.common.utils.Sequences;
import io.druid.query.Query;
import io.druid.query.UnionAllQuery;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.TestIndex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class SimilarityQueryTest extends SketchQueryRunnerTestHelper
{
  @Test
  @SuppressWarnings("unchecked")
  public void testSimilarityQuery() throws Exception
  {
    UnionAllQuery query = new UnionAllQuery(
        null,
        Arrays.<Query>asList(
            SketchQuery.theta(TestIndex.MMAPPED_SPLIT, TestIndex.INTERVAL_TOP)
                       .withDimensionSpecs(
                           Arrays.<DimensionSpec>asList(
                               new DefaultDimensionSpec("market", "market(top)"),
                               new DefaultDimensionSpec("quality", "quality(top)"),
                               new DefaultDimensionSpec("placement", "placement(top)")
                           )
                       )
                       .withMetrics(Arrays.asList("index", "indexMin")),
            SketchQuery.theta(TestIndex.MMAPPED_SPLIT, TestIndex.INTERVAL_BOTTOM)
                       .withDimensionSpecs(DefaultDimensionSpec.toSpec("market", "quality", "placement"))
                       .withMetrics(Arrays.asList("index", "indexMin"))
        ),
        false, 0, 2,
        ImmutableMap.of("postProcessing", new SimilarityProcessingOperator(0, null))
    );

    List result = Sequences.toList(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(3, result.size());
    for (Object x : result) {
      Assert.assertTrue(x instanceof Similarity);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimilarityOnAllColumns() throws Exception
  {
    UnionAllQuery query = new UnionAllQuery(
        null,
        Arrays.<Query>asList(
            SketchQuery.theta(TestIndex.MMAPPED_SPLIT, TestIndex.INTERVAL_TOP),
            SketchQuery.theta(TestIndex.MMAPPED_SPLIT, TestIndex.INTERVAL_BOTTOM)
        ),
        false, 0, 2,
        ImmutableMap.of("postProcessing", new SimilarityProcessingOperator(0, null))
    );
    List result = Sequences.toList(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(5, result.size());
    Assert.assertEquals(
        "Similarity{from='mmapped-split.market', to='mmapped-split.market', relations={similarity=1.0, A=3, B=3, A or B=3, A and B=3, A-B=0, B-A=0}}",
        result.get(0).toString()
    );
    Assert.assertEquals(
        "Similarity{from='mmapped-split.placementish', to='mmapped-split.placementish', relations={similarity=1.0, A=9, B=9, A or B=9, A and B=9, A-B=0, B-A=0}}",
        result.get(1).toString()
    );
    Assert.assertEquals(
        "Similarity{from='mmapped-split.placement', to='mmapped-split.placement', relations={similarity=1.0, A=1, B=1, A or B=1, A and B=1, A-B=0, B-A=0}}",
        result.get(2).toString()
    );
    Assert.assertEquals(
        "Similarity{from='mmapped-split.partial_null_column', to='mmapped-split.partial_null_column', relations={similarity=1.0, A=1, B=1, A or B=1, A and B=1, A-B=0, B-A=0}}",
        result.get(3).toString()
    );
    Assert.assertEquals(
        "Similarity{from='mmapped-split.quality', to='mmapped-split.quality', relations={similarity=1.0, A=9, B=9, A or B=9, A and B=9, A-B=0, B-A=0}}",
        result.get(4).toString()
    );
  }
}
