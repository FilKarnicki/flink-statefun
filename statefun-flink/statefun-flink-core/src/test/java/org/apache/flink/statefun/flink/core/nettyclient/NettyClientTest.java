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

package org.apache.flink.statefun.flink.core.nettyclient;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelDuplexHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelPromise;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.statefun.flink.core.httpfn.TransportClientTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.statefun.flink.core.nettyclient.NettyProtobuf.serializeProtobuf;
import static org.junit.Assert.assertNotNull;

/** This class runs @Test scenarios defined in the parent - {@link TransportClientTest} */
public class NettyClientTest extends TransportClientTest {
    private static FromFunctionNettyTestServer testServer;
    private static PortInfo portInfo;
    CompletableFuture<Integer> callCompletedWithStatusCode;

    @BeforeClass
    public static void beforeClass() {
        testServer = new FromFunctionNettyTestServer();
        portInfo = testServer.runAndGetPortInfo();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        testServer.close();
    }

    @Before
    public void setUp() {
        callCompletedWithStatusCode = new CompletableFuture<>();
    }

    @Override
    public boolean call() throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(createHttpSpec(), "http", portInfo.getHttpPort()));
    }

    @Override
    public boolean callWithTlsFromClasspath()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                "classpath:" + A_CA_CERTS_LOCATION,
                                "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                                "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                                A_SIGNED_CLIENT_KEY_PASSWORD),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callWithTlsFromClasspathWithoutKeyPassword()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                "classpath:" + A_CA_CERTS_LOCATION,
                                "classpath:" + C_SIGNED_CLIENT_CERT_LOCATION,
                                "classpath:" + C_SIGNED_CLIENT_KEY_LOCATION,
                                null),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callWithTlsFromPath()
            throws ExecutionException, InterruptedException, TimeoutException {
        URL caCertsUrl = getClass().getClassLoader().getResource(A_CA_CERTS_LOCATION);
        URL clientCertUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_CERT_LOCATION);
        URL clientKeyUrl = getClass().getClassLoader().getResource(A_SIGNED_CLIENT_KEY_LOCATION);
        assertNotNull(caCertsUrl);
        assertNotNull(clientCertUrl);
        assertNotNull(clientKeyUrl);

        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                caCertsUrl.getPath(),
                                clientCertUrl.getPath(),
                                clientKeyUrl.getPath(),
                                A_SIGNED_CLIENT_KEY_PASSWORD),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callHttpsWithoutAnyTlsSetup()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(createHttpSpec(), "https", portInfo.getHttpsServerTlsOnlyPort()));
    }

    @Override
    protected boolean callHttpsWithOnlyClientSetup()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                null,
                                "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                                "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                                A_SIGNED_CLIENT_KEY_PASSWORD),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callWithUntrustedTlsClient()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                "classpath:" + A_CA_CERTS_LOCATION,
                                "classpath:" + B_SIGNED_CLIENT_CERT_LOCATION,
                                "classpath:" + B_SIGNED_CLIENT_KEY_LOCATION,
                                B_SIGNED_CLIENT_KEY_PASSWORD),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callUntrustedServerWithTlsClient()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec(
                                "classpath:" + B_CA_CERTS_LOCATION,
                                "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                                "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                                A_SIGNED_CLIENT_KEY_PASSWORD),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callWithNoCertGivenButRequired()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec("classpath:" + A_CA_CERTS_LOCATION, null, null, null),
                        "https",
                        portInfo.getHttpsMutualTlsRequiredPort()));
    }

    @Override
    public boolean callWithJustServerSideTls()
            throws ExecutionException, InterruptedException, TimeoutException {
        return callUsingStubsAndCheckSuccess(
                createNettyClient(
                        createSpec("classpath:" + A_CA_CERTS_LOCATION, null, null, null),
                        "https",
                        portInfo.getHttpsServerTlsOnlyPort()));
    }

    private NettyClient createNettyClient(NettyRequestReplySpec spec, String protocol, int port) {
        return NettyClient.from(
                new NettySharedResources(),
                spec,
                URI.create(String.format("%s://localhost:%s", protocol, port)),
                new ChannelDuplexHandler() {
                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
                            throws Exception {
                        callCompletedWithStatusCode.completeExceptionally(cause);
                        super.exceptionCaught(ctx, cause);
                    }

                    @Override
                    public void channelRead(ChannelHandlerContext ctx, Object msg)
                            throws Exception {
                        final FullHttpResponse response =
                                (msg instanceof FullHttpResponse) ? (FullHttpResponse) msg : null;
                        if (response != null) {
                            callCompletedWithStatusCode.complete(response.status().code());
                        } else {
                            callCompletedWithStatusCode.completeExceptionally(
                                    new IllegalStateException(
                                            "the object received by the test is not a FullHttpResponse"));
                        }
                        super.channelRead(ctx, msg);
                    }

                    @Override
                    public void write(
                            ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
                        final NettyRequest request = (NettyRequest) msg;
                        final ByteBuf bodyBuf =
                                serializeProtobuf(
                                        ctx.channel().alloc()::buffer, request.toFunction());
                        DefaultFullHttpRequest http =
                                new DefaultFullHttpRequest(
                                        HttpVersion.HTTP_1_1,
                                        HttpMethod.POST,
                                        request.uri(),
                                        bodyBuf,
                                        new DefaultHttpHeaders(),
                                        NettyHeaders.EMPTY);
                        ctx.writeAndFlush(http);
                    }
                });
    }

    private NettyRequestReplySpec createHttpSpec() {
        return createSpec(null, null, null, null);
    }

    private NettyRequestReplySpec createSpec(
            String trustedCaCerts, String clientCerts, String clientKey, String clientKeyPassword) {
        return new NettyRequestReplySpec(
                Duration.ofMinutes(1L),
                Duration.ofMinutes(1L),
                Duration.ofMinutes(1L),
                1,
                128,
                trustedCaCerts,
                clientCerts,
                clientKey,
                clientKeyPassword,
                new NettyRequestReplySpec.Timeouts());
    }

    private Boolean callUsingStubsAndCheckSuccess(NettyClient nettyClient)
            throws InterruptedException, ExecutionException, TimeoutException {

        NettyRequest nettyRequest =
                new NettyRequest(
                        nettyClient,
                        getFakeMetrics(),
                        getStubRequestSummary(),
                        getEmptyToFunction());

        nettyRequest.start();

        return callCompletedWithStatusCode.get(5, TimeUnit.SECONDS) == 200;
    }
}
