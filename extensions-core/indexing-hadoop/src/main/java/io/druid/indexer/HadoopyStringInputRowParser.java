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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.druid.java.util.common.IAE;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 */
@JsonTypeName("hadoopyString")
public class HadoopyStringInputRowParser implements InputRowParser<Object>
{
  private final StringInputRowParser parser;
  private final String encoding;

  public HadoopyStringInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding
  )
  {
    this.encoding = encoding;
    this.parser = new StringInputRowParser(parseSpec, encoding);
  }

  public HadoopyStringInputRowParser(ParseSpec parseSpec)
  {
    this(parseSpec, null);
  }

  @Override
  public InputRow parse(Object input)
  {
    if (input instanceof String) {
      return parser.parse(input);
    } else if (input instanceof Text) {
      return parser.parse(input.toString());
    } else if (input instanceof BytesWritable) {
      BytesWritable valueBytes = (BytesWritable) input;
      return parser.parse(ByteBuffer.wrap(valueBytes.getBytes(), 0, valueBytes.getLength()));
    } else {
      throw new IAE("can't convert type [%s] to InputRow", input.getClass().getName());
    }
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parser.getParseSpec();
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return getParseSpec().getTimestampSpec();
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return getParseSpec().getDimensionsSpec();
  }

  @Override
  public InputRowParser withDimensionExclusions(Set<String> exclusions)
  {
    ParseSpec parseSpec = parser.getParseSpec();
    return new HadoopyStringInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions)),
        encoding
    );
  }
}
