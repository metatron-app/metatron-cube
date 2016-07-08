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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.util.Lists;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.LineProcessor;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.Parser;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.loading.URIDataPuller;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

public class HadoopURISettlingConfig extends HadoopSettlingConfig
{
  final URI uri;
  final String format;
  final List<String> columns;
  final Parser<String, Object> parser;
  final Map<String, SearchableVersionedDataFinder> pullers;

  @JsonCreator
  public HadoopURISettlingConfig(
      @JsonProperty(value = "uri", required = true) final URI uri,
      @JsonProperty(value = "format", required = true) final String format,
      @JsonProperty(value = "columns", required = true) final List<String> columns,
      @JsonProperty(value = "constColumns", required = true) final List<String> constColumns,
      @JsonProperty(value = "regexColumns", required = true) final List<String> regexColumns,
      @JsonProperty(value = "paramNameColumn", required = true) final String paramNameColumn,
      @JsonProperty(value = "paramValueColumn", required = true) final String paramValueColumn,
      @JsonProperty(value = "typeColumn", required = true) final String aggTypeColumn,
      @JsonProperty(value = "offsetColumn", required = true) final String offset,
      @JsonProperty(value = "sizeColumn", required = true) final String size,
      @JsonProperty(value = "settlingYNColumn") final String settlingYN,
      @JacksonInject Map<String, SearchableVersionedDataFinder> pullers
  )
  {
    super(constColumns, regexColumns, paramNameColumn, paramValueColumn, aggTypeColumn, offset, size, settlingYN);
    this.uri = uri;
    this.format = format;
    this.columns = columns;
    this.parser = getParser();
    this.pullers = pullers;
  }

  @JsonProperty
  public URI getUri()
  {
    return uri;
  }

  @JsonProperty
  public String getFormat()
  {
    return format;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  public Settler setUp(AggregatorFactory[] org)
  {
    final SearchableVersionedDataFinder<URI> pullerRaw = pullers.get(uri.getScheme());
    if (pullerRaw == null) {
      throw new IAE(
          "Unknown loader format[%s].  Known types are %s",
          uri.getScheme(),
          pullers.keySet()
      );
    }
    if (!(pullerRaw instanceof URIDataPuller)) {
      throw new IAE(
          "Cannot load data from location [%s]. Data pulling from [%s] not supported",
          uri,
          uri.getScheme()
      );
    }
    final URIDataPuller puller = (URIDataPuller) pullerRaw;
    final String uriPath = uri.getPath();
    final ByteSource source;

    if (CompressionUtils.isGz(uriPath)) {
      source = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          return CompressionUtils.gzipInputStream(puller.getInputStream(uri));
        }
      };
    } else {
      source = new ByteSource()
      {
        @Override
        public InputStream openStream() throws IOException
        {
          return puller.getInputStream(uri);
        }
      };
    }

    List<Map<String, Object>> mapList = null;
    try {
      mapList = source.asCharSource(Charsets.UTF_8).readLines(
          new LineProcessor<List<Map<String, Object>>>() {
            private List<Map<String, Object>> maps = Lists.newArrayList();

            @Override
            public boolean processLine(String line) throws IOException
            {
              maps.add(parser.parse(line));
              return true;
            }

            @Override
            public List<Map<String, Object>> getResult()
            {
              return maps;
            }
          }
      );
    } catch (IOException e) {
      e.printStackTrace();
    }

    return super.setUp(org, mapList);
  }

  private Parser<String, Object> getParser()
  {
    switch(format.toLowerCase())
    {
      case "csv":
        return new CSVParser(Optional.<String>absent(), columns);
      case "tsv":
        return new DelimitedParser(Optional.<String>absent(), Optional.<String>absent(), columns);
    }

    throw new IAE("format %s is not supported", format);
  }
}
