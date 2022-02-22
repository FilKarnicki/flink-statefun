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

import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.PemUtils;
import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.shaded.netty4.io.netty.handler.ssl.SslHandler;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.Test;

import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public abstract class TransportClientTest {
    protected static final String A_CA_CERTS_LOCATION = "certs/a_caCerts.pem";
    protected static final String B_CA_CERTS_LOCATION = "certs/b_caCerts.pem";
    protected static final String A_SIGNED_CLIENT_CERT_LOCATION = "certs/a_client.crt";
    protected static final String B_SIGNED_CLIENT_CERT_LOCATION = "certs/b_client.crt";
    protected static final String A_SIGNED_CLIENT_KEY_LOCATION = "certs/a_client.key";
    protected static final String B_SIGNED_CLIENT_KEY_LOCATION = "certs/b_client.key";
    private static final String A_SIGNED_CLIENT_KEY_PASSWORD = "test";
    private static final String B_SIGNED_CLIENT_KEY_PASSWORD = A_SIGNED_CLIENT_KEY_PASSWORD;

    @Test
    public void callingTestHttpServiceShouldReturnAFromFunction()
            throws ExecutionException, InterruptedException, TimeoutException {
        FromFunction fromFunction =
                call(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                        .get(5, TimeUnit.SECONDS);

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test
    public void callingTestHttpServiceWithTlsFromPathShouldReturnAFromFunction()
            throws ExecutionException, InterruptedException, TimeoutException {
        FromFunction fromFunction =
                callWithTlsFromPath(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                        .get(5, TimeUnit.SECONDS);

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test
    public void callingTestHttpServiceWithTlsFromClasspathShouldReturnAFromFunction()
            throws ExecutionException, InterruptedException, TimeoutException {
        FromFunction fromFunction =
                callWithTlsFromClasspath(
                                getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                        .get(5, TimeUnit.SECONDS);

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test
    public void callingTestHttpServiceWithJustServerSideTlsShouldReturnAFromFunction() {
        fail("not yet implemented");
    }

    @Test(expected = IllegalStateException.class)
    public void callingTestHttpServiceWithUntrustedTlsClientShouldFail()
            throws ExecutionException, InterruptedException, TimeoutException {
        callWithUntrustedTlsClient(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                .get(5, TimeUnit.SECONDS);
    }

    @Test(expected = IllegalStateException.class)
    public void callingTestHttpServiceWithUntrustedTlsServiceShouldFail()
            throws ExecutionException, InterruptedException, TimeoutException {
        callWithUntrustedTlsService(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                .get(5, TimeUnit.SECONDS);
    }

    public abstract CompletableFuture<FromFunction> call(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction);

    public abstract CompletableFuture<FromFunction> callWithTlsFromPath(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction);

    public abstract CompletableFuture<FromFunction> callWithTlsFromClasspath(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction);

    public abstract CompletableFuture<FromFunction> callWithUntrustedTlsClient(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction);

    public abstract CompletableFuture<FromFunction> callWithUntrustedTlsService(
            ToFunctionRequestSummary requestSummary,
            RemoteInvocationMetrics metrics,
            ToFunction toFunction);

    private static ToFunctionRequestSummary getStubRequestSummary() {
        return new ToFunctionRequestSummary(
                new Address(new FunctionType("ns", "type"), "id"), 1, 0, 1);
    }

    private static ToFunction getEmptyToFunction() {
        return ToFunction.newBuilder().build();
    }

    private static RemoteInvocationMetrics getFakeMetrics() {
        return new RemoteInvocationMetrics() {
            @Override
            public void remoteInvocationFailures() {}

            @Override
            public void remoteInvocationLatency(long elapsed) {}
        };
    }

    public static class FromFunctionNettyTestServer {
        private EventLoopGroup eventLoopGroup;
        private EventLoopGroup workerGroup;

        public static void main(String[] args) {
            PortInfo portInfo = new FromFunctionNettyTestServer().runAndGetPortInfo();
            System.out.println(portInfo.httpPort);
            System.out.println(portInfo.httpsPort);
        }

        public static FromFunction getStubFromFunction() {
            return FromFunction.newBuilder()
                    .setInvocationResult(
                            FromFunction.InvocationResponse.newBuilder()
                                    .addOutgoingEgresses(FromFunction.EgressMessage.newBuilder()))
                    .build();
        }

        public PortInfo runAndGetPortInfo() {
            eventLoopGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();

            try {
                ServerBootstrap httpBootstrap =
                        new ServerBootstrap()
                                .group(eventLoopGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(fromFunctionOkHandler())
                                .option(ChannelOption.SO_BACKLOG, 128)
                                .childOption(ChannelOption.SO_KEEPALIVE, true);

                ServerBootstrap httpsBootstrap =
                        new ServerBootstrap()
                                .group(eventLoopGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(
                                        fromFunctionOkHandler(
                                                "classpath:" + A_CA_CERTS_LOCATION,
                                                "classpath:" + A_SIGNED_CLIENT_CERT_LOCATION,
                                                "classpath:" + A_SIGNED_CLIENT_KEY_LOCATION,
                                                A_SIGNED_CLIENT_KEY_PASSWORD))
                                .option(ChannelOption.SO_BACKLOG, 128)
                                .childOption(ChannelOption.SO_KEEPALIVE, true);

                int httpPort = randomFreePort();
                httpBootstrap.bind(httpPort).sync();

                int httpsPort = randomFreePort();
                httpsBootstrap.bind(httpsPort).sync();

                return new PortInfo(httpPort, httpsPort);
            } catch (Exception e) {
                throw new IllegalStateException("Could not start a test netty server", e);
            }
        }

        public void close() throws InterruptedException {
            eventLoopGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
        }

        private ChannelInitializer<Channel> fromFunctionOkHandler() {
            return new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) {
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
                    pipeline.addLast(stubFromFunctionHandler());
                }
            };
        }

        private ChannelInitializer<Channel> fromFunctionOkHandler(
                String trustedCaCertsLocation,
                String clientCertLocation,
                String clientKeyLocation,
                String keyPassword) {
            return new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) throws IOException {
                    ChannelPipeline pipeline = channel.pipeline();
                    X509ExtendedKeyManager keyManager =
                            PemUtils.loadIdentityMaterial(
                                    ResourceLocator.findNamedResource(clientCertLocation)
                                            .openStream(),
                                    ResourceLocator.findNamedResource(clientKeyLocation)
                                            .openStream(),
                                    keyPassword.toCharArray());
                    X509ExtendedTrustManager trustManager =
                            PemUtils.loadTrustMaterial(
                                    ResourceLocator.findNamedResource(trustedCaCertsLocation)
                                            .openStream());
                    SSLFactory sslFactory =
                            SSLFactory.builder()
                                    .withIdentityMaterial(keyManager)
                                    .withTrustMaterial(trustManager)
                                    .build();

                    pipeline.addLast("tls", new SslHandler(sslFactory.getSSLEngine()));
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
                    pipeline.addLast(stubFromFunctionHandler());
                }
            };
        }

        private SimpleChannelInboundHandler<FullHttpRequest> stubFromFunctionHandler() {
            return new SimpleChannelInboundHandler<FullHttpRequest>() {
                @Override
                protected void channelRead0(
                        ChannelHandlerContext channelHandlerContext,
                        FullHttpRequest fullHttpRequest) {
                    ByteBuf content = Unpooled.copiedBuffer(getStubFromFunction().toByteArray());
                    FullHttpResponse response =
                            new DefaultFullHttpResponse(
                                    HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
                    response.headers()
                            .set(HttpHeaderNames.CONTENT_TYPE, "application/octet-stream");
                    response.headers().set(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                    channelHandlerContext.write(response);
                    channelHandlerContext.flush();
                }
            };
        }

        private int randomFreePort() {
            try (ServerSocket socket = new ServerSocket(0)) {
                return socket.getLocalPort();
            } catch (IOException e) {
                throw new IllegalStateException(
                        "No free ports available for the test netty service to use");
            }
        }
    }

    public static class PortInfo {
        private final int httpPort;
        private final int httpsPort;

        public PortInfo(int httpPort, int httpsPort) {
            this.httpPort = httpPort;
            this.httpsPort = httpsPort;
        }

        public int getHttpPort() {
            return httpPort;
        }

        public int getHttpsPort() {
            return httpsPort;
        }
    }
}
