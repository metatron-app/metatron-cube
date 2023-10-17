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

package io.druid.query.deeplearning;

import io.druid.query.Druids;
import io.druid.segment.TestHelper;
import io.druid.sql.calcite.CalciteQueryTestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class Word2VecQueryTest extends CalciteQueryTestHelper
{
  private static final MiscQueryHook hook = new MiscQueryHook();
  private static TestQuerySegmentWalker walker;

  @BeforeClass
  public static void setUp() throws Exception
  {
    walker = DL4JTestHelper.segmentWalker.withQueryHook(hook);
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }

  @Test
  public void testVector() throws Exception
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("words")
        .virtualColumns(EXPR_VC("vector", "sublist(w2v_test_vector(word), 0, 2)"))
        .columns("word", "vector");

    Object[][] expected = new Object[][]{
        new Object[]{"day", Arrays.asList(-0.18539467453956604, -0.25317487120628357)},
        new Object[]{"big", Arrays.asList(-0.18534202873706818, 0.04932435229420662)},
        new Object[]{"today", Arrays.asList(-0.06812717765569687, 0.008907941170036793)}
    };
    List<Object[]> result = TestHelper.runQuery(builder.streaming(), walker);
    for (Object[] v : result) {
      System.out.println(Arrays.toString(v));
    }
    // changes everytime
    IntStream.range(0, expected.length).forEach(x -> {
      Assert.assertEquals(expected[x][0], result.get(x)[0]);
      Assert.assertTrue(result.get(x)[1] instanceof List);
    });

    testQuery("SELECT word, sublist(w2v_test_vector(word), 0, 2) FROM words", result);
  }

  @Test
  public void testNearest() throws Exception
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("words")
        .virtualColumns(EXPR_VC("nearest", "w2v_test_nearest(word, 2)"))
        .columns("word", "nearest");

    Object[][] expected = new Object[][]{
        new Object[]{"day", Arrays.asList("night", "week")},
        new Object[]{"big", Arrays.asList("good", "such")},
        new Object[]{"today", Arrays.asList("yesterday", "last")}
    };
    List<Object[]> result = TestHelper.runQuery(builder.streaming(), walker);
    for (Object[] v : result) {
      System.out.println(Arrays.toString(v));
    }
    TestHelper.validate(Arrays.asList(expected), result);
    testQuery("SELECT word, w2v_test_nearest(word, 2) FROM words", result);
  }

  @Test
  public void testSimilarity() throws Exception
  {
    Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder()
        .dataSource("words")
        .virtualColumns(EXPR_VC("similarity", "w2v_test_similarity(word, 'best')"))
        .columns("word", "similarity");

    Object[][] expected = new Object[][]{
        new Object[]{"day", 0.040076058357954025},
        new Object[]{"big", 0.21617937088012695},
        new Object[]{"today", 0.10834839195013046}
    };
    List<Object[]> result = TestHelper.runQuery(builder.streaming(), walker);
    for (Object[] v : result) {
      System.out.println(Arrays.toString(v));
    }
    // changes everytime
    IntStream.range(0, expected.length).forEach(x -> {
      Assert.assertEquals(expected[x][0], result.get(x)[0]);
      Assert.assertTrue(result.get(x)[1] instanceof Double);
    });
    testQuery("SELECT word, w2v_test_similarity(word, 'best') FROM words", result);
  }
}
