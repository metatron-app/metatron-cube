/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.java.util.common.parsers.Parser;
import io.druid.data.ParsingFail;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.data.input.TimestampSpec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class StringInputRowParser implements InputRowParser
{
  private static final Charset DEFAULT_CHARSET = Charsets.UTF_8;

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;
  private final Charset charset;

  private CharBuffer chars = null;

  @JsonCreator
  public StringInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("encoding") String encoding
  )
  {
    this.parseSpec = parseSpec;
    this.mapParser = new MapInputRowParser(parseSpec);
    this.parser = parseSpec.makeParser();

    if (encoding != null) {
      this.charset = Charset.forName(encoding);
    } else {
      this.charset = DEFAULT_CHARSET;
    }
  }

  @Deprecated
  public StringInputRowParser(ParseSpec parseSpec)
  {
    this(parseSpec, null);
  }

  @Override
  public InputRow parse(Object input)
  {
    Map<String, Object> event;
    if (input instanceof String) {
      event = parseString((String) input);
    } else if (input instanceof ByteBuffer) {
      event = buildStringKeyMap((ByteBuffer) input);
    } else {
      event = parseString(Objects.toString(input));
    }
    return parseMap(Rows.mergePartitions(event));
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public String getEncoding()
  {
    return charset.name();
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
  @SuppressWarnings("unchecked")
  public InputRowParser withDimensionExclusions(Set exclusions)
  {
    return new StringInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions)),
        getEncoding()
    );
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    final CoderResult coderResult = charset.newDecoder()
                                           .onMalformedInput(CodingErrorAction.REPLACE)
                                           .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                           .decode(input, chars, true);

    Map<String, Object> theMap;
    if (coderResult.isUnderflow()) {
      chars.flip();
      try {
        theMap = parseString(chars.toString());
      }
      finally {
        chars.clear();
      }
    } else {
      throw new ParseException("Failed with CoderResult[%s]", coderResult);
    }
    return theMap;
  }

  private Map<String, Object> parseString(String inputString)
  {
    if (inputString.length() > 0 && inputString.charAt(0) == '\uFEFF') {
      inputString = inputString.substring(1);   // skip BOM
    }
    try {
      return parser.parseToMap(inputString);
    }
    catch (Exception e) {
      throw ParsingFail.propagate(inputString, e);
    }
  }

  private InputRow parseMap(Map<String, Object> theMap)
  {
    try {
      return mapParser.parse(theMap);
    }
    catch (Exception e) {
      throw ParsingFail.propagate(theMap, e);
    }
  }
}
