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

import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.mutable.MutableDouble;

import java.util.List;

public class JavaScriptAggregator extends Aggregator.Abstract<MutableDouble>
{
  static interface ScriptAggregator
  {
    public double aggregate(double current, ObjectColumnSelector[] selectorList);

    public double combine(double a, double b);

    public double reset();

    public void close();
  }

  private final ObjectColumnSelector[] selectorList;
  private final ScriptAggregator script;

  public JavaScriptAggregator(List<ObjectColumnSelector> selectorList, ScriptAggregator script)
  {
    this.selectorList = selectorList.toArray(new ObjectColumnSelector[]{});
    this.script = script;
  }

  @Override
  public MutableDouble aggregate(MutableDouble current)
  {
    if (current == null) {
      current = new MutableDouble(script.reset());
    }
    current.setValue(script.aggregate(current.doubleValue(), selectorList));
    return current;
  }

  public Double get(MutableDouble current)
  {
    return current == null ? null : current.doubleValue();
  }

  @Override
  public void close()
  {
    script.close();
  }
}
