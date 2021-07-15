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

package io.druid.query.aggregation;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.data.TypeResolver;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Functionally similar to an Aggregator. See the Aggregator interface for more comments.
 */
public interface PostAggregator extends TypeResolver.Resolvable
{
  String getName();

  Set<String> getDependentFields();

  Comparator getComparator();

  Processor processor(TypeResolver resolver);

  interface Processor
  {
    String getName();

    Object compute(DateTime timestamp, Map<String, Object> combinedAggregators);
  }

  interface Decorating extends PostAggregator
  {
    boolean needsDecorate();

    PostAggregator decorate(Map<String, AggregatorFactory> aggregators);
  }

  abstract class Abstract implements PostAggregator
  {
    protected abstract class AbstractProcessor implements Processor
    {
      @Override
      public String getName()
      {
        return Abstract.this.getName();
      }
    }
  }

  abstract class Stateless extends Abstract
  {
    private final Supplier<Processor> supplier = Suppliers.memoize(() -> createStateless());

    @Override
    public final Processor processor(TypeResolver resolver)
    {
      return supplier.get();
    }

    protected abstract Processor createStateless();
  }

  public static interface SQLSupport
  {
    PostAggregator rewrite(String name, String fieldName);
  }
}
