package io.druid.server.security;

import io.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public class AuthorizationUtils
{
  public static Access authorize(@Nullable final AuthorizationInfo authorizationInfo, Iterable<String> resourceNames)
  {

    // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
    if (authorizationInfo != null) {
      for (String dataSource : resourceNames) {
        Access authResult = authorizationInfo.isAuthorized(
            new Resource(dataSource, ResourceType.DATASOURCE),
            Action.READ
        );
        if (!authResult.isAllowed()) {
          return authResult;
        }
      }

      return new Access(true);
    } else {
      throw new ISE("Security is enabled but no authorization info found in the request");
    }
  }

}
