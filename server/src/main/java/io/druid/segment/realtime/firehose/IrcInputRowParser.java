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

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.ircclouds.irc.api.domain.messages.ChannelPrivMsg;
import com.metamx.common.Pair;
import io.druid.data.ParsingFail;
import io.druid.data.input.InputRow;
import io.druid.data.input.TimestampSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.joda.time.DateTime;

import java.util.Set;

/**
 * <p><b>Example Usage</b></p>
 * <p/>
 * <p>Decoder definition: <code>wikipedia-decoder.json</code></p>
 * <pre>{@code
 * <p/>
 * {
 *   "type": "wikipedia",
 *   "namespaces": {
 *     "#en.wikipedia": {
 *       "": "main",
 *       "Category": "category",
 *       "Template talk": "template talk",
 *       "Help talk": "help talk",
 *       "Media": "media",
 *       "MediaWiki talk": "mediawiki talk",
 *       "File talk": "file talk",
 *       "MediaWiki": "mediawiki",
 *       "User": "user",
 *       "File": "file",
 *       "User talk": "user talk",
 *       "Template": "template",
 *       "Help": "help",
 *       "Special": "special",
 *       "Talk": "talk",
 *       "Category talk": "category talk"
 *     }
 *   },
 *   "geoIpDatabase": "path/to/GeoLite2-City.mmdb"
 * }
 * }</pre>
 */
@JsonTypeName("irc")
public class IrcInputRowParser implements InputRowParser<Pair<DateTime, ChannelPrivMsg>>
{
  private final ParseSpec parseSpec;
  private final IrcDecoder decoder;

  @JsonCreator
  public IrcInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("decoder") IrcDecoder decoder
  )
  {
    this.parseSpec = parseSpec;
    this.decoder = decoder;
  }

  @JsonProperty
  public IrcDecoder getDecoder()
  {
    return decoder;
  }

  @Override
  public InputRow parse(Pair<DateTime, ChannelPrivMsg> msg)
  {
    try {
      return decoder.decodeMessage(msg.lhs, msg.rhs.getChannelName(), msg.rhs.getText());
    }
    catch (Exception e) {
      throw ParsingFail.propagate(msg, e);
    }
  }

  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
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
    return new IrcInputRowParser(
        parseSpec.withDimensionsSpec(DimensionsSpec.withExclusions(parseSpec.getDimensionsSpec(), exclusions)),
        decoder
    );
  }
}
