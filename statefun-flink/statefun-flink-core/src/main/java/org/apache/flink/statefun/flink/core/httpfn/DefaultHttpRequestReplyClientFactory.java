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
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.statefun.flink.common.ResourceLocator;
import org.apache.flink.statefun.flink.common.SetContextClassLoader;
import org.apache.flink.statefun.flink.common.json.StateFunObjectMapper;
import org.apache.flink.statefun.flink.core.reqreply.ClassLoaderSafeRequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClient;
import org.apache.flink.statefun.flink.core.reqreply.RequestReplyClientFactory;

import javax.annotation.Nullable;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Optional;

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

public final class DefaultHttpRequestReplyClientFactory implements RequestReplyClientFactory {

    public static final DefaultHttpRequestReplyClientFactory INSTANCE =
            new DefaultHttpRequestReplyClientFactory();

    private static final ObjectMapper OBJ_MAPPER = StateFunObjectMapper.create();

    /** lazily initialized by {@link #createTransportClient} */
    @Nullable private volatile OkHttpClient sharedClient;

    private DefaultHttpRequestReplyClientFactory() {}

    @Override
    public RequestReplyClient createTransportClient(
            ObjectNode transportProperties, URI endpointUrl) {
        final DefaultHttpRequestReplyClient client = createClient(transportProperties, endpointUrl);

        if (Thread.currentThread().getContextClassLoader() == getClass().getClassLoader()) {
            return client;
        } else {
            return new ClassLoaderSafeRequestReplyClient(client);
        }
    }

    @Override
    public void cleanup() {
        final OkHttpClient sharedClient = this.sharedClient;
        this.sharedClient = null;
        OkHttpUtils.closeSilently(sharedClient);
    }

    private DefaultHttpRequestReplyClient createClient(
            ObjectNode transportProperties, URI endpointUrl) {
        try (SetContextClassLoader ignored = new SetContextClassLoader(this)) {
            OkHttpClient sharedClient = this.sharedClient;
            if (sharedClient == null) {
                sharedClient = OkHttpUtils.newClient();
                this.sharedClient = sharedClient;
            }
            final OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();

            final DefaultHttpRequestReplyClientSpec transportClientSpec =
                    parseTransportProperties(transportProperties);

            clientBuilder.callTimeout(transportClientSpec.getTimeouts().getCallTimeout());
            clientBuilder.connectTimeout(transportClientSpec.getTimeouts().getConnectTimeout());
            clientBuilder.readTimeout(transportClientSpec.getTimeouts().getReadTimeout());
            clientBuilder.writeTimeout(transportClientSpec.getTimeouts().getWriteTimeout());

            Optional<SSLFactory.Builder> maybeTrustedCaCertsEnabledSslFactoryBuilder =
                    transportClientSpec
                            .getTrustCaCertsOptional()
                            .map(
                                    trustedCaCertsLocation -> {
                                        X509ExtendedTrustManager trustManager =
                                                PemUtils.loadTrustMaterial(
                                                        openStreamOrThrow(
                                                                ResourceLocator.findNamedResource(
                                                                        trustedCaCertsLocation)));
                                        return SSLFactory.builder().withTrustMaterial(trustManager);
                                    });
            Optional<SSLFactory.Builder> maybeMutualTlsEnabledSslFactoryBuilder =
                    maybeTrustedCaCertsEnabledSslFactoryBuilder.flatMap(
                            sslFactoryBuilder ->
                                    transportClientSpec
                                            .getClientCertsOptional()
                                            .map(
                                                    clientCertsLocation -> {
                                                        Optional<String> maybeClientKeyLocation =
                                                                transportClientSpec
                                                                        .getClientKeyOptional();
                                                        if (!maybeClientKeyLocation.isPresent()) {
                                                            throw new IllegalStateException(
                                                                    "if mutual TLS authentication is to be used, both the 'client_certs' and 'client_key' have to be provided (and possibly 'client_key_password')");
                                                        }
                                                        InputStream clientCertInputStream =
                                                                openStreamOrThrow(
                                                                        ResourceLocator
                                                                                .findNamedResource(
                                                                                        clientCertsLocation));
                                                        InputStream clientKeyInputStream =
                                                                openStreamOrThrow(
                                                                        ResourceLocator
                                                                                .findNamedResource(
                                                                                        maybeClientKeyLocation
                                                                                                .get()));
                                                        Optional<String> maybeClientKeyPassword =
                                                                transportClientSpec
                                                                        .getClientKeyPasswordOptional();
                                                        if (maybeClientKeyPassword.isPresent()) {
                                                            return PemUtils.loadIdentityMaterial(
                                                                    clientCertInputStream,
                                                                    clientKeyInputStream,
                                                                    maybeClientKeyPassword
                                                                            .get()
                                                                            .toCharArray());
                                                        } else {
                                                            return PemUtils.loadIdentityMaterial(
                                                                    clientCertInputStream,
                                                                    clientKeyInputStream);
                                                        }
                                                    })
                                            .map(sslFactoryBuilder::withIdentityMaterial));

            if (maybeMutualTlsEnabledSslFactoryBuilder.isPresent()) {
                SSLFactory sslFactory = maybeMutualTlsEnabledSslFactoryBuilder.get().build();
                clientBuilder.sslSocketFactory(
                        sslFactory.getSslSocketFactory(), sslFactory.getTrustManager().get());
            } else if (maybeTrustedCaCertsEnabledSslFactoryBuilder.isPresent()) {
                SSLFactory sslFactory = maybeTrustedCaCertsEnabledSslFactoryBuilder.get().build();
                clientBuilder.sslSocketFactory(
                        sslFactory.getSslSocketFactory(), sslFactory.getTrustManager().get());
            }

            HttpUrl url;
            if (UnixDomainHttpEndpoint.validate(endpointUrl)) {
                UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(endpointUrl);
                url =
                        new HttpUrl.Builder()
                                .scheme("http")
                                .host("unused")
                                .addPathSegment(endpoint.pathSegment)
                                .build();

                configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
            } else {
                url = HttpUrl.get(endpointUrl);
            }

            return new DefaultHttpRequestReplyClient(
                    url, clientBuilder.build(), () -> isShutdown(this.sharedClient));
        }
    }

    private boolean isShutdown(OkHttpClient previousClient) {
        return DefaultHttpRequestReplyClientFactory.this.sharedClient != previousClient;
    }

    private static DefaultHttpRequestReplyClientSpec parseTransportProperties(
            ObjectNode transportClientProperties) {
        try {
            return DefaultHttpRequestReplyClientSpec.fromJson(
                    OBJ_MAPPER, transportClientProperties);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Unable to parse transport client properties when creating client: ", e);
        }
    }

    /** It's ok to use this InputStream without closing it since PemUtils do it for us */
    private InputStream openStreamOrThrow(URL url) {
        try {
            return url.openStream();
        } catch (IOException e) {
            throw new IllegalStateException("Could not open " + url.getPath(), e);
        }
    }
}
