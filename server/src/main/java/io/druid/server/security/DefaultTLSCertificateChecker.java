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

package io.druid.server.security;

import io.druid.java.util.common.logger.Logger;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

public class DefaultTLSCertificateChecker implements TLSCertificateChecker
{
  private static final Logger log = new Logger(DefaultTLSCertificateChecker.class);

  @Override
  public void checkClient(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException
  {
    baseTrustManager.checkClientTrusted(chain, authType, engine);
  }

  @Override
  public void checkServer(
      X509Certificate[] chain,
      String authType,
      SSLEngine engine,
      X509ExtendedTrustManager baseTrustManager
  ) throws CertificateException
  {
    baseTrustManager.checkServerTrusted(chain, authType, engine);
  }
}
