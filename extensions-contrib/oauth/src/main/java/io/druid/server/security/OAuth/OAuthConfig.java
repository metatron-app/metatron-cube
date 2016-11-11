package io.druid.server.security.OAuth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class OAuthConfig
{
  public static final String OAUTH_CONFIG_PATH = "druid.oauth";
  public static final String PATH_KEY = "filterPath";
  public static final String VALIDATE_URL_KEY = "validateURLFormat";
  private static final String BROKER_QUERY_REQUEST_PATH_DEFAULT = "/druid/v2/*";


  @JsonProperty(PATH_KEY)
  private final String path;

  @JsonProperty(VALIDATE_URL_KEY)
  private final String validateURLFormat;

  @JsonCreator
  public OAuthConfig(
      @JsonProperty(PATH_KEY) String path,
      @JsonProperty(VALIDATE_URL_KEY) String validateURLFormat
  )
  {
    this.path = path == null ? BROKER_QUERY_REQUEST_PATH_DEFAULT : path;
    this.validateURLFormat = Preconditions.checkNotNull(validateURLFormat,
        "%s.%s should be specified", OAUTH_CONFIG_PATH, VALIDATE_URL_KEY);
  }

  public String getPath()
  {
    return path;
  }

  public String getValidateURLFormat()
  {
    return validateURLFormat;
  }
}
