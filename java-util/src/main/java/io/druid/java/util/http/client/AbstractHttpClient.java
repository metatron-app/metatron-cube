package io.druid.java.util.http.client;

import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.http.client.response.HttpResponseHandler;

public abstract class AbstractHttpClient implements HttpClient
{
  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return go(request, handler, null);
  }
}
