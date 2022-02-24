/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.statefun.flink.core.httpfn;

import okhttp3.Call;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import static org.apache.flink.statefun.flink.core.httpfn.TransportClientTest.FromFunctionNettyTestServer.getEmptyToFunction;
import static org.junit.Assert.assertNotNull;

/** This class runs @Test scenarios defined in the parent - {@link TransportClientTest} */
public class DefaultHttpRequestReplyClientTest extends TransportClientTest {
  private static final ObjectMapper objectMapper = StateFunObjectMapper.create();
  private static FromFunctionNettyTestServer testServer;
  private static FromFunctionNettyTestServer.PortInfo portInfo;

  @BeforeClass
  public static void beforeClass() {
    testServer = new FromFunctionNettyTestServer();
    portInfo = testServer.runAndGetPortInfo();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    testServer.close();
  }

  @Override
  public boolean call() throws IOException {
    final DefaultHttpRequestReplyClientSpec emptySpec = new DefaultHttpRequestReplyClientSpec();
    return callWithStubs(buildClient(emptySpec, "http", portInfo.getHttpPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callHttpsWithoutAnyTlsSetup() throws IOException {
    final DefaultHttpRequestReplyClientSpec emptySpec = new DefaultHttpRequestReplyClientSpec();
    return callWithStubs(buildClient(emptySpec, "https", portInfo.getHttpsServerTlsOnlyPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  protected boolean callHttpsWithOnlyClientSetup() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    // no truststore settings here
    spec.setClientCerts("classpath:" + A_SIGNED_CLIENT_CERT_LOCATION);
    spec.setClientKey("classpath:" + A_SIGNED_CLIENT_KEY_LOCATION);
    spec.setClientKeyPassword(A_SIGNED_CLIENT_KEY_PASSWORD);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithTlsFromPath() throws IOException {
    URL caCertsUrl = getClass().getClassLoader().getResource(A_CA_CERTS_LOCATION);
    URL clientCertUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_CERT_LOCATION);
    URL clientKeyUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_KEY_LOCATION);
    assertNotNull(caCertsUrl);
    assertNotNull(clientCertUrl);
    assertNotNull(clientKeyUrl);
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts(caCertsUrl.getPath());
    spec.setClientCerts(clientCertUrl.getPath());
    spec.setClientKey(clientKeyUrl.getPath());
    spec.setClientKeyPassword(A_SIGNED_CLIENT_KEY_PASSWORD);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithTlsFromClasspath() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + A_CA_CERTS_LOCATION);
    spec.setClientCerts("classpath:" + A_SIGNED_CLIENT_CERT_LOCATION);
    spec.setClientKey("classpath:" + A_SIGNED_CLIENT_KEY_LOCATION);
    spec.setClientKeyPassword(A_SIGNED_CLIENT_KEY_PASSWORD);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithTlsFromClasspathWithoutKeyPassword() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + A_CA_CERTS_LOCATION);
    spec.setClientCerts("classpath:" + C_SIGNED_CLIENT_CERT_LOCATION);
    spec.setClientKey("classpath:" + C_SIGNED_CLIENT_KEY_LOCATION);
    // no key password required here since C_SIGNED_CLIENT_KEY doesn't need one

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithUntrustedTlsClient() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + A_CA_CERTS_LOCATION); // the client trusts the server
    spec.setClientCerts("classpath:" + B_SIGNED_CLIENT_CERT_LOCATION); // the server won't trust the client
    spec.setClientKey("classpath:" + B_SIGNED_CLIENT_KEY_LOCATION);
    spec.setClientKeyPassword(B_SIGNED_CLIENT_KEY_PASSWORD);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callUntrustedServerWithTlsClient() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + B_CA_CERTS_LOCATION); // the client doesn't trust the server
    spec.setClientCerts("classpath:" + A_SIGNED_CLIENT_CERT_LOCATION); // the server trusts this client
    spec.setClientKey("classpath:" + A_SIGNED_CLIENT_KEY_LOCATION);
    spec.setClientKeyPassword(A_SIGNED_CLIENT_KEY_PASSWORD);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithNoCertGivenButRequired() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + A_CA_CERTS_LOCATION);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsMutualTlsRequiredPort()))
        .execute()
        .isSuccessful();
  }

  @Override
  public boolean callWithJustServerSideTls() throws IOException {
    final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
    spec.setTrustCaCerts("classpath:" + A_CA_CERTS_LOCATION);

    return callWithStubs(buildClient(spec, "https", portInfo.getHttpsServerTlsOnlyPort()))
        .execute()
        .isSuccessful();
  }

  private static Call callWithStubs(DefaultHttpRequestReplyClient defaultHttpRequestReplyClient) {
    return defaultHttpRequestReplyClient.callOnce(getEmptyToFunction());
  }

  private static DefaultHttpRequestReplyClient buildClient(DefaultHttpRequestReplyClientSpec spec, String protocol, int portInfo) {
    return (DefaultHttpRequestReplyClient) DefaultHttpRequestReplyClientFactory.INSTANCE.createTransportClient(
        spec.toJson(objectMapper),
        URI.create(String.format("%s://localhost:%s", protocol, portInfo)));
  }
}
