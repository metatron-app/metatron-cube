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

import com.google.inject.Binder;
import io.druid.sql.guice.SqlBindings;

/**
 * SQL (Calcite) filter conversions for lucene10 (plus the lucene-common ones). Isolated so
 * {@link Lucene10ExtensionModule} carries no reference to {@code io.druid.sql.*} and can be loaded
 * without druid-sql / Calcite (e.g. by an embedded segment writer using only getJacksonModules()).
 */
public final class Lucene10SqlBindings
{
  private Lucene10SqlBindings() {}

  public static void register(Binder binder)
  {
    LuceneCommonSqlBindings.register(binder);
    SqlBindings.addFilterConversion(binder, LuceneKnnVectorConversion.class);
  }
}
