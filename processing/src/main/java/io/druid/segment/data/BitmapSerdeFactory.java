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

package io.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = BitmapSerde.DefaultBitmapSerdeFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "concise", value = ConciseBitmapSerdeFactory.class),
    @JsonSubTypes.Type(name = "roaring", value = RoaringBitmapSerdeFactory.class)
})
public interface BitmapSerdeFactory
{
  public ObjectStrategy<ImmutableBitmap> getObjectStrategy();

  public BitmapFactory getBitmapFactory();

  public BitmapFactory getBitmapFactory(boolean optimizeForSerialization);
}
