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

import io.druid.common.aws.AWSCredentialsConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Verifies the aws-sdk v2 {@link S3StorageDruidModule#getS3Client} provider builds a client for
 * both the static-credentials path (access/secret keys configured) and the default-provider-chain
 * path (no static keys). Credentials are resolved lazily by aws-sdk v2, so the client builds in
 * both cases.
 */
public class TestAWSCredentialsProvider
{
  @Test
  public void testWithFixedAWSKeys()
  {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn("accessKeySample").anyTimes();
    EasyMock.expect(config.getSecretKey()).andReturn("secretKeySample").anyTimes();
    EasyMock.expect(config.getRegion()).andReturn("").anyTimes();
    EasyMock.expect(config.getEndpoint()).andReturn("").anyTimes();
    EasyMock.replay(config);

    S3Client client = module.getS3Client(config);
    Assert.assertNotNull(client);

    EasyMock.verify(config);
  }

  @Test
  public void testWithDefaultProviderChain()
  {
    S3StorageDruidModule module = new S3StorageDruidModule();

    AWSCredentialsConfig config = EasyMock.createMock(AWSCredentialsConfig.class);
    EasyMock.expect(config.getAccessKey()).andReturn("").anyTimes();
    EasyMock.expect(config.getSecretKey()).andReturn("").anyTimes();
    EasyMock.expect(config.getRegion()).andReturn("").anyTimes();
    EasyMock.expect(config.getEndpoint()).andReturn("").anyTimes();
    EasyMock.replay(config);

    S3Client client = module.getS3Client(config);
    Assert.assertNotNull(client);

    EasyMock.verify(config);
  }
}
