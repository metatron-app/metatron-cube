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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.data.input.Row;
import io.druid.query.RowSignature;

/**
 * The logical "not" operator for the "having" clause.
 */
public class NotHavingSpec implements HavingSpec
{
  private HavingSpec havingSpec;

  @JsonCreator
  public NotHavingSpec(@JsonProperty("havingSpec") HavingSpec havingSpec)
  {
    this.havingSpec = havingSpec;
  }

  @JsonProperty("havingSpec")
  public HavingSpec getHavingSpec()
  {
    return havingSpec;
  }

  @Override
  public Predicate<Row> toEvaluator(RowSignature signature)
  {
    return Predicates.not(havingSpec.toEvaluator(signature));
  }

  @Override
  public String toString()
  {
    final StringBuilder sb = new StringBuilder();
    sb.append("NotHavingSpec");
    sb.append("{havingSpec=").append(havingSpec);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NotHavingSpec that = (NotHavingSpec) o;

    if (havingSpec != null ? !havingSpec.equals(that.havingSpec) : that.havingSpec != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return havingSpec != null ? havingSpec.hashCode() : 0;
  }
}
