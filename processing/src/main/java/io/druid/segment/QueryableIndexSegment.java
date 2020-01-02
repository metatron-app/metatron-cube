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

import com.google.common.base.Preconditions;
import io.druid.query.select.Schema;
import org.joda.time.Interval;

import java.io.IOException;

/**
*/
public class QueryableIndexSegment extends AbstractSegment
{
  private final QueryableIndex index;
  private final String identifier;

  public QueryableIndexSegment(final String segmentIdentifier, QueryableIndex index)
  {
    this.index = Preconditions.checkNotNull(index);
    this.identifier = Preconditions.checkNotNull(segmentIdentifier);
  }

  @Override
  public String getIdentifier()
  {
    return identifier;
  }

  @Override
  public Interval getDataInterval()
  {
    return index.getDataInterval();
  }

  @Override
  public QueryableIndex asQueryableIndex(boolean forQuery)
  {
    accessed(forQuery);
    return index;
  }

  @Override
  public StorageAdapter asStorageAdapter(boolean forQuery)
  {
    accessed(forQuery);
    return new QueryableIndexStorageAdapter(index, identifier);
  }

  @Override
  public boolean isIndexed()
  {
    return true;
  }

  @Override
  public int getNumRows()
  {
    return index.getNumRows();
  }

  @Override
  public Schema asSchema(boolean prependTime)
  {
    return index.asSchema(prependTime);
  }

  @Override
  public void close() throws IOException
  {
    // this is kinda nasty
    index.close();
  }
}
