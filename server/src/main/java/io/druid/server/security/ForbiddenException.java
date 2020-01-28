package io.druid.server.security;

public class ForbiddenException extends RuntimeException
{
  public ForbiddenException()
  {
    super("Unauthorized.");
  }

  public ForbiddenException(String msg)
  {
    super(msg);
  }
}
