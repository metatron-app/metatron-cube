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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.data.ParsingFail;
import io.druid.data.input.ExcelParser;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Rows;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class ExcelInputRowParser implements InputRowParser<ExcelRow>
{
  private final ParseSpec parseSpec;
  private final List<String> columns;
  private final List<String> dimensions;
  private final TimestampSpec timestampSpec;
  private final String sheetNameColumn;

  private final Supplier<String[]> columnNameSupplier;

  @JsonCreator
  public ExcelInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("sheetNameColumn") String sheetNameColumn
  )
  {
    this.parseSpec = parseSpec;
    this.columns = columns;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.timestampSpec = parseSpec.getTimestampSpec();
    this.sheetNameColumn = sheetNameColumn;
    this.columnNameSupplier = columns == null ? null : Suppliers.ofInstance(columns.toArray(new String[0]));
  }

  @Override
  public InputRow parse(ExcelRow input)
  {
    try {
      Supplier<String[]> supplier = columnNameSupplier != null ? columnNameSupplier : input;
      Map<String, Object> converted = Rows.mergePartitions(ExcelParser.convert(input.row(), sheetNameColumn, supplier));
      DateTime dateTime = timestampSpec.extractTimestamp(converted);
      return new MapBasedInputRow(dateTime, dimensions, converted);
    }
    catch (Exception e) {
      throw ParsingFail.propagate(input, e);
    }
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return parseSpec.getTimestampSpec();
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return parseSpec.getDimensionsSpec();
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    return new ExcelInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions)),
        columns,
        sheetNameColumn
    );
  }
}
