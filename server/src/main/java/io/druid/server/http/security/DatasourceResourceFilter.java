/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.server.http.security;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ContainerRequest;
import io.druid.server.security.Access;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;

import javax.ws.rs.core.PathSegment;

/**
 * Use this ResourceFilter when the datasource information is present after "datasources" segment in the request Path
 * Here are some example paths where this filter is used -
 * - druid/coordinator/v1/datasources/{dataSourceName}/...
 * - druid/coordinator/v1/metadata/datasources/{dataSourceName}/...
 * - druid/v2/datasources/{dataSourceName}/...
 */
public class DatasourceResourceFilter extends AbstractResourceFilter
{
  @Inject
  public DatasourceResourceFilter(
      AuthorizerMapper authorizerMapper
  )
  {
    super(authorizerMapper);
  }

  @Override
  public ContainerRequest filter(ContainerRequest request)
  {
    final ResourceAction resourceAction = new ResourceAction(
        new Resource(getRequestDatasourceName(request), ResourceType.DATASOURCE),
        getAction(request)
    );

    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        getReq(),
        resourceAction,
        getAuthorizerMapper()
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    return request;
  }

  private String getRequestDatasourceName(ContainerRequest request)
  {
    final String dataSourceName = request.getPathSegments()
                                         .get(
                                             Iterables.indexOf(
                                                 request.getPathSegments(),
                                                 new Predicate<PathSegment>()
                                                 {
                                                   @Override
                                                   public boolean apply(PathSegment input)
                                                   {
                                                     return "datasources".equals(input.getPath());
                                                   }
                                                 }
                                             ) + 1
                                         ).getPath();
    Preconditions.checkNotNull(dataSourceName);
    return dataSourceName;
  }
}
