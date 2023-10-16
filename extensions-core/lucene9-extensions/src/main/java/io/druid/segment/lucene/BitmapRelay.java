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

import com.metamx.collections.bitmap.ImmutableBitmap;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

public class BitmapRelay extends Query
{
  public static Query queryFor(ImmutableBitmap bitmap)
  {
    return bitmap == null ? null : new BitmapRelay(bitmap);
  }

  public static boolean isRelay(Query query)
  {
    if (query instanceof BitmapRelay) {
      return true;
    }
    if (query instanceof BooleanQuery) {
      return isRelay(((BooleanQuery) query).clauses().get(0).getQuery());
    }
    return false;
  }

  public static ImmutableBitmap unwrap(Query query)
  {
    if (query instanceof BitmapRelay) {
      return ((BitmapRelay) query).bitmap;
    }
    if (query instanceof BooleanQuery) {
      return unwrap(((BooleanQuery) query).clauses().get(0).getQuery());
    }
    return null;
  }

  private final ImmutableBitmap bitmap;

  public BitmapRelay(ImmutableBitmap bitmap) {this.bitmap = bitmap;}

  public ImmutableBitmap getBitmap()
  {
    return bitmap;
  }

  @Override
  public String toString(String field)
  {
    return "relay";
  }

  @Override
  public void visit(QueryVisitor visitor)
  {
  }

  @Override
  public boolean equals(Object obj)
  {
    return false;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }
}