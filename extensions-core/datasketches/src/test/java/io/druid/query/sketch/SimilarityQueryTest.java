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
public class SimilarityQueryTest extends SketchQueryRunnerTest
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
        false, 0, 2, 2,
        ImmutableMap.of("postProcessing", new SimilarityProcessingOperator(0, null))
    );

    List result = Sequences.toList(query.run(segmentWalker, Maps.<String, Object>newHashMap()));
    Assert.assertEquals(3, result.size());
    for (Object x : result) {
      Assert.assertTrue(x instanceof Similarity);
    }
  }
}
