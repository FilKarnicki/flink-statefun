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

package org.apache.flink.statefun.e2e.smoke.java;

import io.undertow.Undertow;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.util.PemUtils;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import static org.apache.flink.statefun.e2e.smoke.java.Constants.CMD_INTERPRETER_FN;

public class CommandInterpreterAppServer {
  public static final int PORT = 8000;

  public static void main(String[] args) throws IOException {
    final CommandInterpreter interpreter = new CommandInterpreter();
    final StatefulFunctionSpec FN_SPEC =
        StatefulFunctionSpec.builder(CMD_INTERPRETER_FN)
            .withSupplier(() -> new CommandInterpreterFn(interpreter))
            .withValueSpec(CommandInterpreterFn.STATE)
            .build();
    final StatefulFunctions functions = new StatefulFunctions();
    functions.withStatefulFunction(FN_SPEC);

    final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();

    final InputStream trustedCaCerts = Objects.requireNonNull(CommandInterpreter.class.getClassLoader().getResource("a_ca.pem")).openStream();
    final InputStream aServerCert = Objects.requireNonNull(CommandInterpreter.class.getClassLoader().getResource("a_server.crt")).openStream();
    final InputStream aServerKey = Objects.requireNonNull(CommandInterpreter.class.getClassLoader().getResource("a_server.key")).openStream();
    final SSLFactory sslFactory = SSLFactory.builder()
        .withTrustMaterial(PemUtils.loadTrustMaterial(trustedCaCerts))
        .withIdentityMaterial(PemUtils.loadIdentityMaterial(aServerCert, aServerKey, "test".toCharArray()))
        .build();

    // Use the request-reply handler along with your favorite HTTP web server framework
    // to serve the functions!
    final Undertow httpServer =
        Undertow.builder()
            .addHttpsListener(PORT, "0.0.0.0", sslFactory.getSslContext())
            .setHandler(new UndertowHttpHandler(requestReplyHandler))
            .build();
    httpServer.start();
  }
}
