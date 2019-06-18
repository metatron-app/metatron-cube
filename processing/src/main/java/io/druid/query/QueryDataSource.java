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

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.query.select.Schema;

import java.util.List;

@JsonTypeName("query")
public class QueryDataSource implements DataSource
{
  public static QueryDataSource of(Query query)
  {
    return new QueryDataSource(query);
  }

  public static QueryDataSource of(Query query, Schema schema)
  {
    QueryDataSource dataSource = of(query);
    dataSource.setSchema(schema);
    return dataSource;
  }

  @JsonProperty
  private final Query query;
  private transient Schema schema;  //  result schema of the query

  @JsonCreator
  public QueryDataSource(@JsonProperty("query") Query query)
  {
    this.query = query;
  }

  @Override
  public List<String> getNames()
  {
    return query.getDataSource().getNames();
  }

  @JsonProperty
  public <T> Query<T> getQuery()
  {
    return query;
  }

  public void setSchema(Schema schema)
  {
    this.schema = schema;
  }

  public Schema getSchema()
  {
    return schema;
  }

  @Override
  public String toString() { return query.toString(); }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryDataSource that = (QueryDataSource) o;

    if (!query.equals(that.query)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return query.hashCode();
  }
}
