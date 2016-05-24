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

package io.druid.segment.data;

/**
 */
public class IntersectingOffset implements Offset
{
  private final Offset lhs;
  private final Offset rhs;

  public IntersectingOffset(
      Offset lhs,
      Offset rhs
  )
  {
    this.lhs = lhs;
    this.rhs = rhs;

    findIntersection(lhs.withinBounds(), rhs.withinBounds());
  }

  @Override
  public int getOffset()
  {
    return lhs.getOffset();
  }

  @Override
  public boolean increment()
  {
    final boolean lhsInBound = lhs.increment();
    final boolean rhsInBound = rhs.increment();

    return findIntersection(lhsInBound, rhsInBound);
  }

  private boolean findIntersection(boolean lhsInBound, boolean rhsInBound)
  {
    if (!(lhsInBound && rhsInBound)) {
      return false;
    }

    int lhsOffset = lhs.getOffset();
    int rhsOffset = rhs.getOffset();

    while (lhsOffset != rhsOffset) {
      while (lhsOffset < rhsOffset) {
        lhs.increment();
        if (!lhs.withinBounds()) {
          return false;
        }

        lhsOffset = lhs.getOffset();
      }

      while (rhsOffset < lhsOffset) {
        rhs.increment();
        if (!rhs.withinBounds()) {
          return false;
        }

        rhsOffset = rhs.getOffset();
      }
    }
    return true;
  }

  @Override
  public boolean withinBounds()
  {
    return lhs.withinBounds() && rhs.withinBounds();
  }

  @Override
  public Offset clone()
  {
    final Offset lhsClone = lhs.clone();
    final Offset rhsClone = rhs.clone();
    return new IntersectingOffset(lhsClone, rhsClone);
  }
}
