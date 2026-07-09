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

package io.druid.segment.loading;

import com.google.common.collect.ImmutableSet;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * The pruning soundness contract of {@link SegmentPruneIndex#requiredValues}: it may only report a hard
 * single-column requirement (Selector / In / And-of-those); Or / Not / cross-column give null so a segment is
 * never skipped when it could still match.
 */
public class SegmentPruneIndexTest
{
  private static final String COL = "source_sha256";

  private static SelectorDimFilter sel(String dim, String val)
  {
    return new SelectorDimFilter(dim, val, null);
  }

  private static InDimFilter in(String dim, String... vals)
  {
    return new InDimFilter(dim, Arrays.asList(vals), null);
  }

  @Test
  public void selectorOnColumn()
  {
    Assert.assertEquals(ImmutableSet.of("x"), SegmentPruneIndex.requiredValues(sel(COL, "x"), COL));
  }

  @Test
  public void selectorOnOtherColumnGivesNothing()
  {
    Assert.assertNull(SegmentPruneIndex.requiredValues(sel("file_id", "x"), COL));
  }

  @Test
  public void inOnColumn()
  {
    Assert.assertEquals(ImmutableSet.of("a", "b"), SegmentPruneIndex.requiredValues(in(COL, "a", "b"), COL));
  }

  @Test
  public void andTakesTheConstrainingChild()
  {
    // must match both -> the source_sha256 conjunct is a hard requirement
    final AndDimFilter and = AndDimFilter.of(sel(COL, "x"), sel("file_id", "y"));
    Assert.assertEquals(ImmutableSet.of("x"), SegmentPruneIndex.requiredValues(and, COL));
  }

  @Test
  public void andIntersectsMultipleConstraints()
  {
    // In[a,b,c] AND =a  ->  the row must be a
    final AndDimFilter and = AndDimFilter.of(in(COL, "a", "b", "c"), sel(COL, "a"));
    Assert.assertEquals(ImmutableSet.of("a"), SegmentPruneIndex.requiredValues(and, COL));
  }

  @Test
  public void orGivesNothing()
  {
    // a non-column branch could match -> no hard requirement, never prune
    final OrDimFilter or = new OrDimFilter(Arrays.asList(sel(COL, "x"), sel("file_id", "y")));
    Assert.assertNull(SegmentPruneIndex.requiredValues(or, COL));
  }

  @Test
  public void canSkipHonorsTheRequirement()
  {
    final SegmentPruneIndex idx = new SegmentPruneIndex(Arrays.asList(COL));
    idx.putForTest("seg1", COL, ImmutableSet.of("a", "b"));

    // required value present -> keep; absent -> skip
    Assert.assertFalse(idx.canSkip("seg1", sel(COL, "a")));
    Assert.assertTrue(idx.canSkip("seg1", sel(COL, "zzz")));
    Assert.assertFalse(idx.canSkip("seg1", in(COL, "zzz", "b")));   // b is present
    Assert.assertTrue(idx.canSkip("seg1", in(COL, "y", "z")));      // neither present

    // unindexed segment or unprunable filter -> never skip
    Assert.assertFalse(idx.canSkip("unknown", sel(COL, "zzz")));
    Assert.assertFalse(idx.canSkip("seg1", null));
    final Set<String> ignored = SegmentPruneIndex.requiredValues(sel("file_id", "zzz"), COL);
    Assert.assertNull(ignored);
    Assert.assertFalse(idx.canSkip("seg1", sel("file_id", "zzz")));
  }
}
