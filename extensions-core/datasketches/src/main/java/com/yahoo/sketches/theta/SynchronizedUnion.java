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

package com.yahoo.sketches.theta;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.theta.CompactSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Union;

/**
 */
public class SynchronizedUnion extends Union
{
  private final Union delegate;

  public SynchronizedUnion(Union delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public synchronized void update(Sketch sketchIn)
  {
    delegate.update(sketchIn);
  }

  @Override
  public synchronized void update(Memory mem)
  {
    delegate.update(mem);
  }

  @Override
  public synchronized void update(long datum)
  {
    delegate.update(datum);
  }

  @Override
  public synchronized void update(double datum)
  {
    delegate.update(datum);
  }

  @Override
  public synchronized void update(String datum)
  {
    delegate.update(datum);
  }

  @Override
  public synchronized void update(byte[] data)
  {
    delegate.update(data);
  }

  @Override
  public synchronized void update(int[] data)
  {
    delegate.update(data);
  }

  @Override
  public synchronized void update(char[] data)
  {
    delegate.update(data);
  }

  @Override
  public synchronized void update(long[] data)
  {
    delegate.update(data);
  }

  @Override
  public synchronized CompactSketch getResult(boolean b, WritableMemory memory)
  {
    return delegate.getResult(b, memory);
  }

  @Override
  public synchronized CompactSketch getResult()
  {
    return delegate.getResult();
  }

  @Override
  public synchronized byte[] toByteArray()
  {
    return delegate.toByteArray();
  }

  @Override
  public synchronized void reset()
  {
    delegate.reset();
  }

  @Override
  public synchronized Family getFamily()
  {
    return delegate.getFamily();
  }

  @Override
  public synchronized boolean isSameResource(Memory mem)
  {
    return delegate.isSameResource(mem);
  }

  @Override
  boolean isEmpty()
  {
    return delegate.isEmpty();
  }

  @Override
  long[] getCache()
  {
    return delegate.getCache();
  }

  @Override
  int getRetainedEntries(boolean valid)
  {
    return delegate.getRetainedEntries(valid);
  }

  @Override
  short getSeedHash()
  {
    return delegate.getSeedHash();
  }

  @Override
  long getThetaLong()
  {
    return delegate.getThetaLong();
  }
}
