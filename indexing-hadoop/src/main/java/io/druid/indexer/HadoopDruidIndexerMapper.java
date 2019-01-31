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

import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import io.druid.data.ParserInitializationFail;
import io.druid.data.ParsingFail;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.indexer.hadoop.HadoopAwareParser;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopDruidIndexerMapper.class);

  private static final int INVALID_LOG_THRESHOLD = 10;

  protected HadoopDruidIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;

  private Counter indexedRows;
  private Counter invalidRows;
  private Counter oobRows;
  private Counter errRows;

  private boolean oobLogged;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();

    indexedRows = context.getCounter("druid.internal", "indexed-row-num");
    invalidRows = context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER);
    oobRows = context.getCounter("druid.internal", "oob-row-num");
    errRows = context.getCounter("druid.internal", "err-row-num");

    setupHadoopAwareParser(parser, context);
  }

  private void setupHadoopAwareParser(InputRowParser parser, Context context) throws IOException
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
    try {
      final InputRow inputRow = parseInputRow(value, parser);
      if (inputRow == null) {
        return;
      }
      if (!granularitySpec.bucketIntervals().isPresent()
          || granularitySpec.bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()))
                            .isPresent()) {
        indexedRows.increment(1);
        innerMap(inputRow, value, context);
      } else {
        oobRows.increment(1);
        if (!oobLogged) {
          log.info("Out of bound row [%s]. will be ignored in next", inputRow);
          oobLogged = true;
        }
      }
    }
    catch (Throwable e) {
      errRows.increment(1);
      if (config.isIgnoreInvalidRows()) {
        handelInvalidRow(value, e);
        return; // we're ignoring this invalid row
      }
      if (e instanceof ParsingFail) {
        Object target = ((ParsingFail) e).getInput();
        e = e.getCause() == null ? e : e.getCause();
        log.info(e, "Ignoring invalid row due to parsing fail of %s", target == null ? value : target);
      }
      if (e instanceof IOException) {
        throw (IOException) e;
      }
      if (e instanceof InterruptedException) {
        throw (InterruptedException) e;
      }
      throw Throwables.propagate(e);
    }
  }

  private void handelInvalidRow(Object value, Throwable e)
  {
    invalidRows.increment(1);
    if (invalidRows.getValue() <= INVALID_LOG_THRESHOLD) {
      log.warn(
          e,
          "Ignoring invalid [%d]th row [%s] due to parsing error.. %s", invalidRows.getValue(), value,
          invalidRows.getValue() == INVALID_LOG_THRESHOLD ? "will not be logged further" : ""
      );
    }
    if (e instanceof ParserInitializationFail) {
      throw (ParserInitializationFail) e;   // invalid configuration, etc.. fail early
    }
  }

  @SuppressWarnings("unchecked")
  private InputRow parseInputRow(Object value, InputRowParser parser)
  {
    return value instanceof InputRow ? (InputRow) value : parser.parse(value);
  }

  protected abstract void innerMap(InputRow inputRow, Object value, Context context)
      throws IOException, InterruptedException;
}
