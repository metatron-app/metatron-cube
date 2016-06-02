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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import com.metamx.common.RE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class HadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopDruidIndexerMapper.class);

  protected HadoopDruidIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;

  private Function<InputRow, Iterable<InputRow>> generator;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();
    final String nameField = config.getSchema().getTuningConfig().getJobProperties().get("hynix.columns.param");
    final String valueField = config.getSchema().getTuningConfig().getJobProperties().get("hynix.columns.value");

    if (nameField != null && valueField != null) {
      generator = new Function<InputRow, Iterable<InputRow>>()
      {
        @Override
        public Iterable<InputRow> apply(final InputRow input)
        {
          final Map<String, Object> mapRow = ((MapBasedInputRow)input).getEvent();

          Object nameObject = mapRow.get(nameField);
          Object valueObject = mapRow.get(valueField);
          Preconditions.checkArgument((nameObject instanceof List) && (valueObject instanceof List),
              "param and value columns specified in hynix.columns should contain array data");
          final List names = (List)nameObject;
          final List values = (List)valueObject;
          Preconditions.checkArgument(names.size() == values.size(),
              "number of elements in param and value array should be the same");

          List<Pair<String, String>> validPairs = Lists.newArrayList();
          for (int idx = 0; idx < names.size(); idx++)
          {
            String name = (String)names.get(idx);
            String value = (String)values.get(idx);
            if (isNumeric(value)) {
              validPairs.add(Pair.of(name, value));
            }
          }

          return Iterables.transform(
              validPairs, new Function<Pair<String,String>, InputRow>()
              {
                @Override
                public InputRow apply(Pair<String, String> pair)
                {
                  mapRow.put(nameField, pair.lhs);
                  mapRow.put(valueField, pair.rhs);
                  return input;
                }
              }
          );
        }
      };
    } else {
      generator = new Function<InputRow, Iterable<InputRow>>()
      {
        @Override
        public Iterable<InputRow> apply(InputRow input)
        {
          return ImmutableList.of(input);
        }
      };
    }
  }

  private boolean isNumeric(String str)
  {
    try {
      Float.parseFloat(str);
      if ("NaN".equals(str)) {
        throw new NumberFormatException();
      }
    }
    catch (NumberFormatException e) {
      return false;
    }
    return true;
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
  protected void map(
      Object key, Object value, Context context
  ) throws IOException, InterruptedException
  {
    try {
      final InputRow inputRow = parseRow(value, context);
      if (inputRow == null) {
        return;
      }
      if (!granularitySpec.bucketIntervals().isPresent()
          || granularitySpec.bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()))
                            .isPresent()) {
        for (InputRow row : generator.apply(inputRow)) {
          innerMap(row, value, context);
        }
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  private InputRow parseRow(Object value, Context context) {
    try {
      return parseInputRow(value, parser);
    }
    catch (Exception e) {
      if (config.isIgnoreInvalidRows()) {
        log.debug(e, "Ignoring invalid row [%s] due to parsing error", value.toString());
        context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
        return null; // we're ignoring this invalid row
      } else {
        throw e;
      }
    }
  }

  public final static InputRow parseInputRow(Object value, InputRowParser parser)
  {
    if (parser instanceof StringInputRowParser && value instanceof Text) {
      //Note: This is to ensure backward compatibility with 0.7.0 and before
      //HadoopyStringInputRowParser can handle this and this special case is not needed
      //except for backward compatibility
      return ((StringInputRowParser) parser).parse(value.toString());
    } else if (value instanceof InputRow) {
      return (InputRow) value;
    } else {
      return parser.parse(value);
    }
  }

  abstract protected void innerMap(InputRow inputRow, Object value, Context context)
      throws IOException, InterruptedException;

}
