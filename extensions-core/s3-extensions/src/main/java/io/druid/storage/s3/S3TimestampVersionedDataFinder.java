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

package io.druid.storage.s3;

import io.druid.java.util.common.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.data.SearchableVersionedDataFinder;
import io.druid.java.util.common.RetryUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * This is implemented explicitly for URIExtractionNamespaceFunctionFactory
 * If you have a use case for this interface beyond URIExtractionNamespaceFunctionFactory please bring it up in the dev list.
 */
public class S3TimestampVersionedDataFinder extends S3DataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  @Inject
  public S3TimestampVersionedDataFinder(S3Client s3Client)
  {
    super(s3Client);
  }

  /**
   * Gets the latest version using the "parent" of uri as a prefix. The "parent" of uri is evaluated assuming '/'
   * delimited paths. If the uri path ends with '/', the path is assumed to be the parent.
   *
   * @param uri     The URI of interest whose "parent" will be searched as a key prefix for the latest version
   * @param pattern The pattern matcher to determine if a *key* is of interest. This will match against the portion of the key that is beyond the URI path,
   *                not just the equivalent "filename" like some other implementations. A null value matches everything.
   *                If there is a "/" delimiter between the uri path and the file match, it is ignore. Patterns should **not** account for a leading "/" unless there's a double "/" for some reason
   *
   * @return A URI to the most recently modified object which matched the pattern.
   */
  @Override
  public URI getLatestVersion(final URI uri, final @Nullable Pattern pattern)
  {
    try {
      return RetryUtils.retry(
          new Callable<URI>()
          {
            @Override
            public URI call() throws Exception
            {
              final S3Coords coords = new S3Coords(checkURI(uri));
              long mostRecent = Long.MIN_VALUE;
              URI latest = null;
              String continuationToken = null;
              do {
                final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(coords.bucket)
                    .prefix(coords.path)
                    .delimiter("/");
                if (continuationToken != null) {
                  requestBuilder.continuationToken(continuationToken);
                }
                final ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
                for (S3Object storageObject : response.contents()) {
                  String keyString = storageObject.key().substring(coords.path.length());
                  if (keyString.startsWith("/")) {
                    keyString = keyString.substring(1);
                  }
                  if (pattern != null && !pattern.matcher(keyString).matches()) {
                    continue;
                  }
                  final long latestModified = storageObject.lastModified().toEpochMilli();
                  if (latestModified >= mostRecent) {
                    mostRecent = latestModified;
                    latest = new URI(String.format("s3://%s/%s", coords.bucket, storageObject.key()));
                  }
                }
                continuationToken = Boolean.TRUE.equals(response.isTruncated()) ? response.nextContinuationToken() : null;
              } while (continuationToken != null);
              return latest;
            }
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public List<URI> getAllVersions(final URI uri, final @Nullable Pattern pattern)
  {
    try {
      return RetryUtils.retry(
          new Callable<List<URI>>()
          {
            @Override
            public List<URI> call() throws Exception
            {
              final S3Coords coords = new S3Coords(checkURI(uri));
              final List<URI> uriList = Lists.newArrayList();
              String continuationToken = null;
              do {
                final ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                    .bucket(coords.bucket)
                    .prefix(coords.path)
                    .delimiter("/");
                if (continuationToken != null) {
                  requestBuilder.continuationToken(continuationToken);
                }
                final ListObjectsV2Response response = s3Client.listObjectsV2(requestBuilder.build());
                for (S3Object storageObject : response.contents()) {
                  String keyString = storageObject.key().substring(coords.path.length());
                  if (keyString.startsWith("/")) {
                    keyString = keyString.substring(1);
                  }
                  if (pattern == null || pattern.matcher(keyString).matches()) {
                    uriList.add(new URI(String.format("s3://%s/%s", coords.bucket, storageObject.key())));
                  }
                }
                continuationToken = Boolean.TRUE.equals(response.isTruncated()) ? response.nextContinuationToken() : null;
              } while (continuationToken != null);
              return uriList;
            }
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Class<URI> getDataDescriptorClass()
  {
    return URI.class;
  }
}
