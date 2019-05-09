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

package io.druid.indexer;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.ParserInitializationFail;
import io.druid.data.ParsingFail;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.hadoop.HadoopAwareParser;
import io.druid.indexer.hadoop.HadoopInputContext.MapperContext;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopDruidIndexerMapper.class);

  private static final int INVALID_LOG_THRESHOLD = 10;

  protected HadoopDruidIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;
  private List<Interval> intervals;

  private Counter indexedRows;
  private Counter oobRows;
  private Counter errRows;
  private Counter nullRows;

  private boolean oobLogged;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();

    Optional<SortedSet<Interval>> buckets = granularitySpec.bucketIntervals();
    if (buckets.isPresent()) {
      intervals = JodaUtils.condenseIntervals(buckets.get());
    }

    indexedRows = context.getCounter("druid.internal", "indexed-row-num");
    oobRows = context.getCounter("druid.internal", "oob-row-num");
    errRows = context.getCounter("druid.internal", "err-row-num");
    nullRows = context.getCounter("druid.internal", "null-row-num");

    setupHadoopAwareParser(parser, new MapperContext(context));
  }

  private void setupHadoopAwareParser(InputRowParser parser, MapperContext context) throws IOException
  {
    if (parser instanceof HadoopAwareParser) {
      ((HadoopAwareParser) parser).setup(context);
    }
    if (parser instanceof InputRowParser.Delegated) {
      setupHadoopAwareParser(((InputRowParser.Delegated) parser).getDelegate(), context);
    }
  }

  public HadoopDruidIndexerConfig getConfig()
  {
    return config;
  }

  public InputRowParser getParser()
  {
    return parser;
  }

  @Override
  protected void map(Object key, Object value, Context context) throws IOException, InterruptedException
  {
    final InputRow inputRow;
    try {
      inputRow = parseInputRow(value, parser);
    }
    catch (Throwable e) {
      Throwables.propagateIfInstanceOf(e, Error.class);
      Throwables.propagateIfInstanceOf(e, ParserInitializationFail.class);  // invalid configuration, etc.. fail early

      handleInvalidRow(value, e);

      if (config.isIgnoreInvalidRows()) {
        return; // we're ignoring this invalid row
      }
      Throwables.propagateIfInstanceOf(e, IOException.class);
      Throwables.propagateIfInstanceOf(e, InterruptedException.class);
      throw Throwables.propagate(e);
    }
    process(inputRow, context);
  }

  private void process(InputRow inputRow, Context context) throws IOException, InterruptedException
  {
    if (inputRow == null) {
      nullRows.increment(1);
      return;
    }
    if (intervals == null || JodaUtils.contains(inputRow.getTimestampFromEpoch(), intervals)) {
      indexedRows.increment(1);
      innerMap(inputRow, context);
    } else {
      oobRows.increment(1);
      if (!oobLogged) {
        log.info("Out of bound row [%s]. will be ignored in next", inputRow);
        oobLogged = true;
      }
    }
  }

  private void handleInvalidRow(Object value, Throwable e)
  {
    errRows.increment(1);
    if (e instanceof ParsingFail) {
      value = ((ParsingFail) e).getInput() == null ? value : ((ParsingFail) e).getInput();
      e = e.getCause() == null ? e : e.getCause();
    }
    if (errRows.getValue() <= INVALID_LOG_THRESHOLD) {
      log.warn(
          e,
          "Invalid row [%s] ([%d]th) due to parsing error.. %s", value, errRows.getValue(),
          errRows.getValue() == INVALID_LOG_THRESHOLD ? "will not be logged further" : ""
      );
    }
  }

  @SuppressWarnings("unchecked")
  private InputRow parseInputRow(Object value, InputRowParser parser)
  {
    return value instanceof InputRow ? (InputRow) value : parser.parse(value);
  }

  protected abstract void innerMap(InputRow inputRow, Context context)
      throws IOException, InterruptedException;
}
