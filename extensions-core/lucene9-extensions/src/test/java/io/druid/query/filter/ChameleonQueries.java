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

package io.druid.query.filter;

import com.google.common.collect.Iterables;
import io.druid.query.Druids;
import io.druid.query.Schema;
import io.druid.query.SchemaQuery;
import io.druid.segment.Lucene9TestHelper;
import io.druid.segment.LuceneTestRunner;
import io.druid.segment.TestHelper;
import io.druid.segment.lucene.KnnVectorFilter;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ChameleonQueries extends LuceneTestRunner
{
  @Override
  protected TestQuerySegmentWalker segmentWalker()
  {
    return Lucene9TestHelper.segmentWalker;
  }

  @Test
  public void testSchema()
  {
    Schema schema = Iterables.getOnlyElement(runQuery(SchemaQuery.of("chameleon")));
    Assert.assertEquals("[__time, NUMBER, vector]", schema.getColumnNames().toString());
    Assert.assertEquals("[long, dimension.string, array.float]", schema.getColumnTypes().toString());
    Assert.assertEquals(
        "{vector={knn=knn.vector}}", schema.getDescriptors().toString()
    );
  }

  @Test
  public void testKnnFilter()
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("chameleon")
        .columns("NUMBER", "vector", "score");

    Object[][] expected = new Object[][]{
        {"11272", Arrays.asList(0.5235608, 0.44156343), 0.99604577},
        {"11273", Arrays.asList(0.5235496, 0.441608), 0.9960515},
        {"11275", Arrays.asList(0.5236085, 0.4415981), 0.99604756},
        {"11276", Arrays.asList(0.5233764, 0.44162035), 0.9960609},
        {"11277", Arrays.asList(0.52329844, 0.441492), 0.9960497},
        {"11278", Arrays.asList(0.52339643, 0.4415921), 0.9960568},
        {"11281", Arrays.asList(0.5232792, 0.4414288), 0.9960432},
        {"11282", Arrays.asList(0.52322507, 0.44143406), 0.99604625},
        {"11283", Arrays.asList(0.5233347, 0.4416579), 0.9960672},
        {"11284", Arrays.asList(0.5232767, 0.44144332), 0.99604493}
    };
    builder.filters(new KnnVectorFilter("vector.knn", new float[]{0.5F, 0.5F}, 10, "score"));
    TestHelper.validate(Arrays.asList(expected), runQuery(builder.streaming()));

    expected = new Object[][]{
        {"17109", Arrays.asList(0.6986746, 0.7375302), 0.9985917},
        {"17110", Arrays.asList(0.69870335, 0.73773277), 0.99857664},
        {"17112", Arrays.asList(0.6990159, 0.7378504), 0.9985684},
        {"17113", Arrays.asList(0.6992028, 0.7378952), 0.9985654},
        {"17114", Arrays.asList(0.6988469, 0.737547), 0.9985909},
        {"17115", Arrays.asList(0.69896543, 0.73778206), 0.9985734},
        {"17117", Arrays.asList(0.69865865, 0.7377513), 0.9985751},
        {"17118", Arrays.asList(0.6991229, 0.73761463), 0.9985864},
        {"17120", Arrays.asList(0.6990927, 0.7379153), 0.9985637},
        {"17121", Arrays.asList(0.6988718, 0.737996), 0.99855715}
    };
    builder.filters(new KnnVectorFilter("vector.knn", new float[]{0.7F, 0.7F}, 10, "score"));
    TestHelper.validate(Arrays.asList(expected), runQuery(builder.streaming()));
  }
}