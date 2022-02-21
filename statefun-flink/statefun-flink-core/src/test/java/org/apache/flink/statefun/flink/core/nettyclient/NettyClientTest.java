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

import org.apache.flink.statefun.flink.core.httpfn.TransportClientTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.URI;
import java.time.Duration;

/** This class runs @Test scenarios defined in the parent - {@link TransportClientTest} */
public class NettyClientTest extends TransportClientTest {
    private static FromFunctionNettyTestServer testServer;
    private static final Duration ONE_MINUTE = Duration.ofMinutes(1L);
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
    public boolean call() {

        NettyClient nettyClient =
                NettyClient.from(
                        new NettySharedResources(),
                        new NettyRequestReplySpec(
                                ONE_MINUTE,
                                ONE_MINUTE,
                                ONE_MINUTE,
                                1,
                                128,
                                null,
                                null,
                                null,
                                null,
                                new NettyRequestReplySpec.Timeouts()),
                        URI.create("http://localhost:" + portInfo.getHttpPort()));
        nettyClient.call(
                getStubRequestSummary(),
                getFakeMetrics(),
                getEmptyToFunction()); // .get(5, TimeUnit.SECONDS);
        return false;
    }

    @Override
    public boolean callHttpsWithoutAnyTlsSetup() {
        return false;
    }

    @Override
    protected boolean callHttpsWithOnlyClientSetup() {
        return false;
    }

    @Override
    public boolean callWithTlsFromPath() {
        return false;
    }

    @Override
    public boolean callWithTlsFromClasspath() {
        return false;
    }

    @Override
    public boolean callWithTlsFromClasspathWithoutKeyPassword() {
        return false;
    }

    @Override
    public boolean callWithUntrustedTlsClient() {
        return false;
    }

    @Override
    public boolean callUntrustedServerWithTlsClient() {
        return false;
    }

    @Override
    public boolean callWithNoCertGivenButRequired() {
        return false;
    }

    @Override
    public boolean callWithJustServerSideTls() {
        return false;
    }
}
