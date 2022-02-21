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

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.apache.flink.shaded.netty4.io.netty.channel.*;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.*;
import org.apache.flink.statefun.flink.core.metrics.RemoteInvocationMetrics;
import org.apache.flink.statefun.flink.core.reqreply.ToFunctionRequestSummary;
import org.apache.flink.statefun.sdk.Address;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public abstract class TransportClientTest {
    @Test
    public void callingTestHttpServiceShouldReturnAFromFunction() {
        FromFunction fromFunction =
                call(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction()).join();

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test
    public void callingTestHttpServiceWithTlsFromPathShouldReturnAFromFunction() {
        FromFunction fromFunction =
                callWithTlsFromPath(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                        .join();

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test
    public void callingTestHttpServiceWithTlsFromClasspathShouldReturnAFromFunction() {
        FromFunction fromFunction =
                callWithTlsFromClasspath(
                                getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                        .join();

        assertEquals(FromFunctionNettyTestServer.getStubFromFunction(), fromFunction);
    }

    @Test(expected = IllegalStateException.class)
    public void callingTestHttpServiceWithUntrustedTlsClientShouldFail() {
        callWithUntrustedTlsClient(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                .join();
    }

    @Test(expected = IllegalStateException.class)
    public void callingTestHttpServiceWithUntrustedTlsServiceShouldFail() {
        callWithUntrustedTlsService(getStubRequestSummary(), getFakeMetrics(), getEmptyToFunction())
                .join();
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
        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;

        public static FromFunction getStubFromFunction() {
            return FromFunction.newBuilder()
                    .setInvocationResult(
                            FromFunction.InvocationResponse.newBuilder()
                                    .addOutgoingEgresses(FromFunction.EgressMessage.newBuilder()))
                    .build();
        }

        public int runAndGetHttpPort() {
            bossGroup = new NioEventLoopGroup();
            workerGroup = new NioEventLoopGroup();

            try {
                ServerBootstrap httpBootstrap =
                        new ServerBootstrap()
                                .group(bossGroup, workerGroup)
                                .channel(NioServerSocketChannel.class)
                                .childHandler(fromFunctionOkHandler())
                                .option(ChannelOption.SO_BACKLOG, 128)
                                .childOption(ChannelOption.SO_KEEPALIVE, true);

                int httpPort = randomFreePort();
                // Bind and start to accept incoming connections.
                httpBootstrap.bind(httpPort).sync();

                return httpPort;
            } catch (Exception e) {
                throw new IllegalStateException("Could not start a test netty server", e);
            }
        }

        private ChannelInitializer<Channel> fromFunctionOkHandler() {
            return new ChannelInitializer<Channel>() {
                @Override
                protected void initChannel(Channel channel) {
                    ChannelPipeline pipeline = channel.pipeline();
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(Integer.MAX_VALUE));
                    // pipeline.addLast("ssl", new
                    // SslHandler(SslContext.newSer()));
                    pipeline.addLast(
                            new SimpleChannelInboundHandler<FullHttpRequest>() {
                                @Override
                                protected void channelRead0(
                                        ChannelHandlerContext channelHandlerContext,
                                        FullHttpRequest fullHttpRequest) {
                                    ByteBuf content =
                                            Unpooled.copiedBuffer(
                                                    getStubFromFunction().toByteArray());
                                    FullHttpResponse response =
                                            new DefaultFullHttpResponse(
                                                    HttpVersion.HTTP_1_1,
                                                    HttpResponseStatus.OK,
                                                    content);
                                    response.headers()
                                            .set(
                                                    HttpHeaderNames.CONTENT_TYPE,
                                                    "application/octet-stream");
                                    response.headers()
                                            .set(
                                                    HttpHeaderNames.CONTENT_LENGTH,
                                                    content.readableBytes());
                                    channelHandlerContext.write(response);
                                    channelHandlerContext.flush();
                                }
                            });
                }
            };
        }

        public void close() throws InterruptedException {
            bossGroup.shutdownGracefully().sync();
            workerGroup.shutdownGracefully().sync();
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
}
