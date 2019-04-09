/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.output;

public interface ForwardConstants
{
  String FORMAT = "format";
  String CLEANUP = "cleanup";

  // format
  String INDEX_FORMAT = "index";
  String JSON_FORMAT = "json";
  String CSV_FORMAT = "csv";
  String TSV_FORMAT = "tsv";
  String EXCEL_FORMAT = "excel";

  // index-format
  String DATASOURCE = "dataSource";
  String REGISTER_TABLE = "registerTable";
  String TEMPORARY = "temporary";
  String SCHEMA = "schema";
  String INTERVAL = "interval";
  String WAIT_TIMEOUT = "waitTimeout";
  String ASSERT_LOADED = "assertLoaded";

  // json
  String COLUMNS = "columns";
  String WRAP_AS_LIST = "wrapAsList";

  // xsv
  String WITH_HEADER = "withHeader";
  String NULL_VALUE = "nullValue";
  String CHARSET = "charset";

  // excel
  String FLUSH_INTERVAL = "flushInterval";
  String MAX_ROWS_PER_SHEET = "maxRowsPerSheet";

  // dummy
  String LOCAL_TEMP_PATH = "/__temporary";
  String LOCAL_TEMP_URL = "file:///__temporary";
}
