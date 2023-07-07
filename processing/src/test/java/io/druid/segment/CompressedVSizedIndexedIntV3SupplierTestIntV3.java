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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.CompressedVintsSupplierTest;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IntsValues;
import io.druid.segment.data.VintValues;
import io.druid.segment.data.WritableSupplier;
import org.junit.After;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class CompressedVSizedIndexedIntV3SupplierTestIntV3 extends CompressedVintsSupplierTest
{
  @Before
  public void setUpSimple(){
    vals = Arrays.asList(
        new int[1],
        new int[]{1, 2, 3, 4, 5},
        new int[]{6, 7, 8, 9, 10},
        new int[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
    );

    indexedSupplier = CompressedVIntsSupplierV3.fromIterable(
        Iterables.transform(
            vals,
            new Function<int[], IndexedInts>()
            {
              @Override
              public IndexedInts apply(int[] input)
              {
                return VintValues.fromArray(input, 20);
              }
            }
        ), 2, 20, ByteOrder.nativeOrder(),
        CompressedObjectStrategy.CompressionStrategy.LZ4
    );
  }

  @After
  public void teardown(){
    indexedSupplier = null;
    vals = null;
  }

  @Override
  protected WritableSupplier<IntsValues> fromByteBuffer(ByteBuffer buffer, ByteOrder order)
  {
    return CompressedVIntsSupplierV3.from(
        buffer, ByteOrder.nativeOrder()
    );
  }
}