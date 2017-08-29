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

package io.druid.firehose.uri;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DelimitedParseSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.DefaultTimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

public class URIFirehoseFactoryTest
{
  private Injector injector;
  private ObjectMapper mapper;

  @Before
  public void setUpModule() throws Exception
  {
    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.<Module>of()
        ),
        ImmutableList.of(
            new Module() {
              @Override
              public void configure(Binder binder) {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            },
            new URIFirehoseDruidModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
  }

  @Test
  public void testNoFilter() throws IOException, URISyntaxException
  {
    URL dataURL = this.getClass().getClassLoader().getResource("data");

    String uriFH = "{\"type\":\"uri\",\"URI\":\"" + dataURL.toURI() + "\"}";

    FirehoseFactory factory = mapper.readValue(uriFH, FirehoseFactory.class);

    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("product")),
                null,
                null
            ),
            null,
            null,
            ImmutableList.of("time", "product", "qty")
        ),
        "UTF-8"
    );

    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(3360, rows.size());
  }

  @Test
  public void testFiltered() throws IOException, URISyntaxException
  {
    URL dataURL = this.getClass().getClassLoader().getResource("data");

    String uriFH = "{\"type\":\"uri\",\"pattern\":\".*.txt\",\"URI\":\"" + dataURL.toURI() + "\"}";

    FirehoseFactory factory = mapper.readValue(uriFH, FirehoseFactory.class);

    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("product")),
                null,
                null
            ),
            null,
            null,
            ImmutableList.of("time", "product", "qty")
        ),
        "UTF-8"
    );

    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(480, rows.size());
  }

  @Test
  public void testHttp() throws IOException, URISyntaxException
  {
    String uriFH = "{\"type\":\"uri\",\"pattern\":\".*.txt\",\"URI\":\"http://localhost/~ktpark/simple_test_data.txt\"}";

    FirehoseFactory factory = mapper.readValue(uriFH, FirehoseFactory.class);

    StringInputRowParser parser = new StringInputRowParser(
        new DelimitedParseSpec(
            new DefaultTimestampSpec(
                "time",
                "auto",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("product")),
                null,
                null
            ),
            null,
            null,
            ImmutableList.of("time", "product", "qty")
        ),
        "UTF-8"
    );

    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore()) {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(480, rows.size());
  }
}
