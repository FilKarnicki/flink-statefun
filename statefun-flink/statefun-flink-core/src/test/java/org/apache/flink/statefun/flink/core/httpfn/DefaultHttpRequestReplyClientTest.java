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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

/** This class runs @Test scenarios defined in the parent - {@link TransportClientTest} */
public class DefaultHttpRequestReplyClientTest extends TransportClientTest {
    private static final ObjectMapper objectMapper = StateFunObjectMapper.create();
    private static FromFunctionNettyTestServer testServer;
    private static PortInfo portInfo;

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
    public CompletableFuture<FromFunction> call(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction) {
        RequestReplyClient defaultHttpRequestReplyClient =
                DefaultHttpRequestReplyClientFactory.INSTANCE.createTransportClient(
                        objectMapper.createObjectNode(),
                        URI.create("http://localhost:" + portInfo.getHttpPort()));

        return defaultHttpRequestReplyClient.call(requestSummary, metrics, toFunction);
    }

    @Override
    public CompletableFuture<FromFunction> callWithTlsFromPath(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction) {
        return null;
    }

    @Override
    public CompletableFuture<FromFunction> callWithTlsFromClasspath(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction) {

        final DefaultHttpRequestReplyClientSpec spec = new DefaultHttpRequestReplyClientSpec();
        spec.setTrustCaCerts("classpath:" + A_CA_CERTS);
        spec.setClientCerts("classpath:" + A_SIGNED_CLIENT_CERT_LOCATION);
        spec.setClientKey("classpath:" + A_SIGNED_CLIENT_KEY_LOCATION);

        RequestReplyClient defaultHttpRequestReplyClient =
                DefaultHttpRequestReplyClientFactory.INSTANCE.createTransportClient(
                        spec.toJson(objectMapper),
                        URI.create("https://localhost:" + portInfo.getHttpsPort()));

        return defaultHttpRequestReplyClient.call(requestSummary, metrics, toFunction);
    }

    @Override
    public CompletableFuture<FromFunction> callWithUntrustedTlsClient(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction) {
        return null;
    }

    @Override
    public CompletableFuture<FromFunction> callWithUntrustedTlsService(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction) {
        return null;
    }
}
