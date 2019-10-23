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

package io.druid.segment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Doubles;
import io.druid.java.util.common.guava.Sequence;
import io.druid.collections.StupidPool;
import io.druid.common.utils.Sequences;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.JoinQueryConfig;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.topn.TopNQueryEngine;
import io.druid.segment.column.Column;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Assert;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class TestHelper
{
  private static final IndexMerger INDEX_MERGER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals(QuerySegmentWalker.class.getName())) {
              return TestIndex.segmentWalker;
            } else if (valueId.equals(ExecutorService.class.getName())) {
              return TestIndex.segmentWalker.getExecutor();
            } else if (valueId.equals(QueryToolChestWarehouse.class.getName())) {
              return TestIndex.segmentWalker;
            } else if (valueId.equals(JoinQueryConfig.class.getName())) {
              return TestIndex.segmentWalker.getQueryConfig().getJoin();
            }
            return null;
          }
        }
    );
    INDEX_IO = new IndexIO(
        JSON_MAPPER
    );
    INDEX_MERGER = new IndexMerger(JSON_MAPPER, INDEX_IO);
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  public static ObjectMapper getTestObjectMapper()
  {
    return JSON_MAPPER;
  }


  public static IndexMerger getTestIndexMerger()
  {
    return INDEX_MERGER;
  }

  public static IndexMergerV9 getTestIndexMergerV9()
  {
    return INDEX_MERGER_V9;
  }

  public static IndexIO getTestIndexIO()
  {
    return INDEX_IO;
  }

  public static ObjectMapper getObjectMapper() {
    return JSON_MAPPER;
  }

  public static String printObjectPretty(Object object)
  {
    try {
      return JSON_MAPPER.writer(new DefaultPrettyPrinter()).writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static ObjectMapper makeJsonMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ObjectMapper.class.getName(), mapper)
    );
    return mapper;
  }

  public static <T> Iterable<T> revert(Iterable<T> input) {
    return Lists.reverse(Lists.newArrayList(input));
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Sequence<Result<T>> results)
  {
    assertResults(expectedResults, Sequences.toList(results), "");
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    assertResults(expectedResults, results, "");
  }

  public static <T> void assertExpectedResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> results,
      String failMsg
  )
  {
    assertResults(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Iterable<T> results)
  {
    assertObjects(expectedResults, results, "");
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Iterable<T> results, String failMsg)
  {
    assertObjects(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Sequence<T> results)
  {
    assertExpectedObjects(expectedResults, results, "");
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Sequence<T> results, String failMsg)
  {
    assertObjects(expectedResults, Sequences.toList(results), failMsg);
  }

  private static <T> void assertResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> actualResults,
      String failMsg
  )
  {
    Iterator<? extends Result> resultsIter = actualResults.iterator();
    Iterator<? extends Result> expectedResultsIter = expectedResults.iterator();

    while (resultsIter.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        Assert.assertEquals(failMsg, expectedNext, next);
      } else {
        assertResult(failMsg, (Result) expectedNext, (Result) next);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              failMsg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static <T> void assertObjects(Iterable<T> expectedResults, Iterable<T> actualResults, String msg)
  {
    Iterator resultsIter = actualResults.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    int index = 0;
    while (resultsIter.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();

      String failMsg = msg + "-" + index++;
      String failMsg2 = String.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg);

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        assertRow(failMsg, (Row) expectedNext, (Row) next);
      } else {
        Assert.assertEquals(failMsg, expectedNext, next);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              msg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static void assertRow(String msg, Row expected, Row actual)
  {
    // Custom equals check to get fuzzy comparison of numerics, useful because different groupBy strategies don't
    // always generate exactly the same results (different merge ordering / float vs double)
    Assert.assertEquals(String.format("%s: timestamp", msg), expected.getTimestamp(), actual.getTimestamp());

    final Map<String, Object> expectedMap = ((MapBasedRow) expected).getEvent();
    final Map<String, Object> actualMap = ((MapBasedRow) actual).getEvent();

//    Assert.assertEquals(String.format("%s: map keys", msg), expectedMap.keySet(), actualMap.keySet());
    for (final String key : expectedMap.keySet()) {
      final Object expectedValue = expectedMap.get(key);
      final Object actualValue = actualMap.get(key);

      if (expectedValue instanceof Float || expectedValue instanceof Double) {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            ((Number) expectedValue).doubleValue(),
            ((Number) actualValue).doubleValue(),
            ((Number) expectedValue).doubleValue() * 1e-6
        );
      } else if (expectedValue instanceof Double[]) {
        Assert.assertArrayEquals(
            String.format("%s: key[%s]", msg, key),
            Doubles.toArray(Arrays.asList((Double[]) expectedValue)),
            Doubles.toArray(Arrays.asList((Double[]) actualValue)),
            0.0001
        );
      } else if (expectedValue != null && expectedValue.getClass().isArray()) {
        int length = Array.getLength(expectedValue);
        for (int i = 0; i < length; i++) {
          Assert.assertEquals(
              String.format("%s: key[%s.%d]", msg, key, i),
              Array.get(expectedValue, i),
              Array.get(actualValue, i)
          );
        }
      } else {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            expectedValue,
            actualValue
        );
      }
    }
  }

  private static void assertResult(String msg, Result<?> expected, Result actual)
  {
    Assert.assertEquals(msg, expected.getTimestamp(), actual.getTimestamp());
    Object o1 = expected.getValue();
    Object o2 = actual.getValue();
    Assert.assertEquals(msg, expected.getValue(), actual.getValue());
  }

  public static TopNQueryEngine testTopNQueryEngine()
  {
    return new TopNQueryEngine(
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(1024 * 1024);
              }
            }
        )
    );
  }

  public static List<InputRow> createInputRows(String[] columnNames, Object[]... values)
  {
    int timeIndex = Arrays.asList(columnNames).indexOf(Column.TIME_COLUMN_NAME);
    List<InputRow> expected = Lists.newArrayList();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          theVals.put(columnNames[i], value[i]);
        }
      }
      DateTime timestamp = timeIndex < 0 ? new DateTime(0) : new DateTime(value[timeIndex]);
      expected.add(new MapBasedInputRow(timestamp, Arrays.asList(columnNames), theVals));
    }
    return expected;
  }
}
