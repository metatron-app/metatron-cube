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

package io.druid.query.search.search;

import java.util.Comparator;

/**
 */
abstract class MergeSortSpec implements SearchSortSpec
{
  private final GenericSearchSortSpec ordering;
  private final Comparator<SearchHit> mergeComparator;

  public MergeSortSpec(GenericSearchSortSpec ordering)
  {
    this.ordering = ordering;
    this.mergeComparator = ordering == null ? null : ordering.getComparator();
  }

  @Override
  public Comparator<SearchHit> getMergeComparator()
  {
    return mergeComparator;
  }

  @Override
  public byte[] getCacheKey()
  {
    return toString().getBytes();
  }

  @Override
  public boolean equals(Object other) {
    return toString().equals(other.toString());
  }

  @Override
  public String toString()
  {
    return ordering == null ? "" : "(" + ordering.toString() + ")";
  }
}
