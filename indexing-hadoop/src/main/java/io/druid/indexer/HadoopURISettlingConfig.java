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
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.metamx.common.CompressionUtils;
import com.metamx.common.IAE;
import com.metamx.common.parsers.CSVParser;
import com.metamx.common.parsers.DelimitedParser;
import com.metamx.common.parsers.Parser;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.loading.URIDataPuller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class HadoopURISettlingConfig extends HadoopSettlingConfig
{
  final URI uri;
  final String pattern;
  final String format;
  final List<String> columns;
  final Parser<String, Object> parser;
  final Map<String, SearchableVersionedDataFinder> pullers;

  private final Pattern compiledPattern;

  @JsonCreator
  public HadoopURISettlingConfig(
      @JsonProperty(value = "uri", required = true) final URI uri,
      @JsonProperty(value = "pattern", required = false) final String pattern,
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
    this.pattern = pattern;
    this.compiledPattern = (pattern == null) ? null : Pattern.compile(pattern);
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
  public String getPattern()
  {
    return pattern;
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
    final List<URI> uriList = pullerRaw.getAllVersions(uri, compiledPattern);

    List<Map<String, Object>> maps = Lists.newArrayList();

    for (URI fileURI: uriList) {
      final String uriPath = fileURI.getPath();
      final InputStream source;

      try {
        if (CompressionUtils.isGz(uriPath)) {
          source = CompressionUtils.gzipInputStream(puller.getInputStream(fileURI));
        } else {
          source = puller.getInputStream(fileURI);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(source));

        String line;
        while ((line = reader.readLine()) != null) {
          maps.add(parser.parse(line));
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return super.setUp(org, maps);
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
