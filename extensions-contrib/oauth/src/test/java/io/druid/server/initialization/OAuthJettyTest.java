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
package io.druid.server.initialization;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.server.DruidNode;
import io.druid.server.initialization.jetty.JettyServerInitializer;
import io.druid.server.security.OAuth.OAuthConfig;
import io.druid.server.security.OAuth.OAuthExtensionsModule;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.HttpURLConnection;
import java.net.URL;

public class OAuthJettyTest extends BaseJettyTest
{
  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("test", "localhost", null)
                );
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);

                Jerseys.addResource(binder, RequestResource.class);
                Jerseys.addResource(binder, NonFilteredResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            },
            new OAuthExtensionsModule()
        )
    );
  }

  public static void setOAuthProperties()
  {
    System.setProperty(OAuthConfig.OAUTH_CONFIG_PATH + "." + OAuthConfig.PATH_KEY, "/druid/v2/*");
    System.setProperty(OAuthConfig.OAUTH_CONFIG_PATH + "." + OAuthConfig.VALIDATE_URL_KEY,
        "http://emn-g02-01:8180/oauth/check_token?token=%s");
  }

  @Before
  @Override
  public void setup() throws Exception
  {
    setOAuthProperties();
    super.setup();
  }

  @Path("/druid/v2/test")
  public static class RequestResource
  {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response get()
    {
      return Response.ok("hello").build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response post()
    {
      return Response.ok("hello").build();
    }
  }

  @Path("/druid/v3/test")
  public static class NonFilteredResource
  {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response get()
    {
      return Response.ok("hello").build();
    }

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    public Response post()
    {
      return Response.ok("hello").build();
    }
  }

  @Ignore
  @Test
  public void testExtensionOAuthFilter() throws Exception
  {
    String goodKey = "Bearer a08fe0a3-1971-4a5a-b2cd-85295eb78890";
    String badKey = "Bearer 7ade7f55-8b51-4a59-908c-918e557bcxxx";

    // test GOOD key
    URL url = new URL("http://localhost:" + port + "/druid/v2/test");
    HttpURLConnection get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty("Authorization", goodKey);
    Assert.assertEquals(HttpServletResponse.SC_OK, get.getResponseCode());

    // test BAD key
    get = (HttpURLConnection) url.openConnection();
    get.setRequestProperty("Authorization", badKey);
    Assert.assertEquals(HttpServletResponse.SC_UNAUTHORIZED, get.getResponseCode());
  }

  @Test
  public void testNonFilteredURL() throws Exception
  {
    URL url = new URL("http://localhost:" + port + "/druid/v3/test");
    HttpURLConnection get = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpServletResponse.SC_OK, get.getResponseCode());
  }
}
