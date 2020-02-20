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

package io.druid.indexing.overlord.supervisor;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(EasyMockRunner.class)
public class SupervisorResourceTest extends EasyMockSupport
{
  private static final TestSupervisorSpec SPEC1 = new TestSupervisorSpec(
      "id1",
      null,
      Collections.singletonList("datasource1")
  );

  private static final TestSupervisorSpec SPEC2 = new TestSupervisorSpec(
      "id2",
      null,
      Collections.singletonList("datasource2")
  );

  private static final Set<String> SUPERVISOR_IDS = ImmutableSet.of(SPEC1.getId(), SPEC2.getId());

  @Mock
  private TaskMaster taskMaster;

  @Mock
  private SupervisorManager supervisorManager;

  @Mock
  private HttpServletRequest request;

  private SupervisorResource supervisorResource;

  @Before
  public void setUp()
  {
    supervisorResource = new SupervisorResource(
        taskMaster,
        new AuthorizerMapper(null)
        {
          @Override
          public Authorizer getAuthorizer(String name)
          {
            return (authenticationResult, resource, action) -> {
              if (authenticationResult.getIdentity().equals("druid")) {
                return Access.OK;
              } else {
                if (resource.getName().equals("datasource2")) {
                  return new Access(false, "not authorized.");
                } else {
                  return Access.OK;
                }
              }
            };
          }
        }
    );
  }


  @Test
  public void testSpecPost() throws Exception
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, Collections.singletonList("datasource1"));

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.createOrUpdateAndStartSupervisor(spec)).andReturn(true);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specPost(spec, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAll() throws Exception
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC1.getId())).andReturn(Optional.of(SPEC1));
    EasyMock.expect(supervisorManager.getSupervisorSpec(SPEC2.getId())).andReturn(Optional.of(SPEC2));
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAll(null, request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(SUPERVISOR_IDS, response.getEntity());
    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAll(null, request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllFull()
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager));
    EasyMock.expect(supervisorManager.getSupervisorIds()).andReturn(SUPERVISOR_IDS).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).anyTimes();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).anyTimes();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAll("", request);
    verifyAll();

    Assert.assertEquals(200, response.getStatus());
    List<Map<String, Object>> specs = (List<Map<String, Object>>) response.getEntity();
    boolean b = true;
    for (Map<String, Object> spec : specs) {
      if ((!"id1".equals(spec.get("id")) || !SPEC1.equals(spec.get("spec"))) &&
          (!"id2".equals(spec.get("id")) || !SPEC2.equals(spec.get("spec")))) {
        b = false;
        break;
      }
    }
    Assert.assertTrue(
        b
    );
  }

  @Test
  public void testSpecGet() throws Exception
  {
    SupervisorSpec spec = new TestSupervisorSpec("my-id", null, null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id")).andReturn(Optional.of(spec));
    EasyMock.expect(supervisorManager.getSupervisorSpec("my-id-2")).andReturn(Optional.absent());
    replayAll();

    Response response = supervisorResource.specGet("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(spec, response.getEntity());

    response = supervisorResource.specGet("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGet("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetStatus() throws Exception
  {
    SupervisorReport report = new SupervisorReport("id", DateTime.now())
    {
      @Override
      public Object getPayload()
      {
        return null;
      }
    };

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id")).andReturn(Optional.of(report));
    EasyMock.expect(supervisorManager.getSupervisorStatus("my-id-2")).andReturn(Optional.<SupervisorReport>absent());
    replayAll();

    Response response = supervisorResource.specGetStatus("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(report, response.getEntity());

    response = supervisorResource.specGetStatus("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetStatus("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testShutdown() throws Exception
  {
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id")).andReturn(true);
    EasyMock.expect(supervisorManager.stopAndRemoveSupervisor("my-id-2")).andReturn(false);
    replayAll();

    Response response = supervisorResource.shutdown("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.shutdown("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.shutdown("my-id");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetAllHistory() throws Exception
  {
    List<VersionedSupervisorSpec> versions1 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id1", null, Collections.singletonList("datasource1")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource1")),
            "tombstone"
        )
    );
    List<VersionedSupervisorSpec> versions2 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, Collections.singletonList("datasource2")),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource2")),
            "v3"
        )
    );
    List<VersionedSupervisorSpec> versions3 = ImmutableList.of(
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v1"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v2"
        ),
        new VersionedSupervisorSpec(
            new NoopSupervisorSpec(null, null),
            "tombstone"
        ),
        new VersionedSupervisorSpec(
            new TestSupervisorSpec("id2", null, Collections.singletonList("datasource3")),
            "v3"
        )
    );
    Map<String, List<VersionedSupervisorSpec>> history = new HashMap<>();
    history.put("id1", versions1);
    history.put("id2", versions2);
    history.put("id3", versions3);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history);
    EasyMock.expect(supervisorManager.getSupervisorSpec("id1")).andReturn(Optional.of(SPEC1)).atLeastOnce();
    EasyMock.expect(supervisorManager.getSupervisorSpec("id2")).andReturn(Optional.of(SPEC2)).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).atLeastOnce();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).atLeastOnce();
    request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    replayAll();

    Response response = supervisorResource.specGetAllHistory(request);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(history, response.getEntity());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.absent());
    replayAll();

    response = supervisorResource.specGetAllHistory(request);
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }

  @Test
  public void testSpecGetHistory() throws Exception
  {
    List<VersionedSupervisorSpec> versions = ImmutableList.of(
        new VersionedSupervisorSpec(null, "v1"),
        new VersionedSupervisorSpec(null, "v2")
    );
    Map<String, List<VersionedSupervisorSpec>> history = Maps.newHashMap();
    history.put("id1", versions);
    history.put("id2", null);

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.getSupervisorHistory()).andReturn(history).times(2);
    replayAll();

    Response response = supervisorResource.specGetHistory("id1");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(versions, response.getEntity());

    response = supervisorResource.specGetHistory("id3");

    Assert.assertEquals(404, response.getStatus());

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.specGetHistory("id1");
    verifyAll();

    Assert.assertEquals(503, response.getStatus());
  }


  @Test
  public void testReset() throws Exception
  {
    Capture<String> id1 = Capture.newInstance();
    Capture<String> id2 = Capture.newInstance();
    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.of(supervisorManager)).times(2);
    EasyMock.expect(supervisorManager.resetSupervisor(EasyMock.capture(id1), EasyMock.anyObject(DataSourceMetadata.class))).andReturn(true);
    EasyMock.expect(supervisorManager.resetSupervisor(EasyMock.capture(id2), EasyMock.anyObject(DataSourceMetadata.class))).andReturn(false);
    replayAll();

    Response response = supervisorResource.reset("my-id");

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("id", "my-id"), response.getEntity());

    response = supervisorResource.reset("my-id-2");

    Assert.assertEquals(404, response.getStatus());
    Assert.assertEquals("my-id", id1.getValue());
    Assert.assertEquals("my-id-2", id2.getValue());
    verifyAll();

    resetAll();

    EasyMock.expect(taskMaster.getSupervisorManager()).andReturn(Optional.<SupervisorManager>absent());
    replayAll();

    response = supervisorResource.shutdown("my-id");

    Assert.assertEquals(503, response.getStatus());
    verifyAll();
  }


  private static class TestSupervisorSpec implements SupervisorSpec
  {
    private final String id;
    private final Supervisor supervisor;
    private final List<String> dataSources;

    public TestSupervisorSpec(String id, Supervisor supervisor, List<String> dataSources)
    {
      this.id = id;
      this.supervisor = supervisor;
      this.dataSources = dataSources;
    }

    @Override
    public String getId()
    {
      return id;
    }

    @Override
    public Supervisor createSupervisor()
    {
      return supervisor;
    }

    @Override
    public List<String> getDataSources()
    {
      return dataSources;
    }
  }
}
