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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.statefun.flink.core.httpfn.OkHttpUnixSocketBridge.configureUnixDomainSocket;

public final class DefaultHttpRequestReplyClientFactory implements RequestReplyClientFactory {

  public static final DefaultHttpRequestReplyClientFactory INSTANCE = new DefaultHttpRequestReplyClientFactory();

  private static final ObjectMapper OBJ_MAPPER = StateFunObjectMapper.create();

  /** lazily initialized by {@link #createTransportClient} */
  @Nullable
  private volatile OkHttpClient sharedClient;

  private DefaultHttpRequestReplyClientFactory() {
  }

  @Override
  public RequestReplyClient createTransportClient(ObjectNode transportProperties, URI endpointUrl) {
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

  private DefaultHttpRequestReplyClient createClient(ObjectNode transportProperties, URI endpointUrl) {
    try (SetContextClassLoader ignored = new SetContextClassLoader(this)) {
      OkHttpClient sharedClient = this.sharedClient;
      if (sharedClient == null) {
        sharedClient = OkHttpUtils.newClient();
        this.sharedClient = sharedClient;
      }
      final OkHttpClient.Builder clientBuilder = sharedClient.newBuilder();

      final DefaultHttpRequestReplyClientSpec transportClientSpec = parseTransportProperties(transportProperties);

      clientBuilder.callTimeout(transportClientSpec.getTimeouts().getCallTimeout());
      clientBuilder.connectTimeout(transportClientSpec.getTimeouts().getConnectTimeout());
      clientBuilder.readTimeout(transportClientSpec.getTimeouts().getReadTimeout());
      clientBuilder.writeTimeout(transportClientSpec.getTimeouts().getWriteTimeout());

      Optional<SSLFactory> maybeSslFactory = buildSslFactory(
          transportClientSpec.getTrustCaCertsOptional(),
          transportClientSpec.getClientCertsOptional(),
          transportClientSpec.getClientKeyOptional(),
          transportClientSpec.getClientKeyPasswordOptional());

      maybeSslFactory.ifPresent(sslFactory -> clientBuilder.sslSocketFactory(
          sslFactory.getSslSocketFactory(),
          sslFactory.getTrustManager().orElseThrow(() -> new IllegalStateException("BUG: The ssl factory is present, so the trust manager needs to be present too."))));

      HttpUrl url;
      if (UnixDomainHttpEndpoint.validate(endpointUrl)) {
        UnixDomainHttpEndpoint endpoint = UnixDomainHttpEndpoint.parseFrom(endpointUrl);
        url = new HttpUrl.Builder().scheme("http").host("unused").addPathSegment(endpoint.pathSegment).build();

        configureUnixDomainSocket(clientBuilder, endpoint.unixDomainFile);
      } else {
        url = HttpUrl.get(endpointUrl);
      }

      return new DefaultHttpRequestReplyClient(url, clientBuilder.build(), () -> isShutdown(this.sharedClient));
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Optional<SSLFactory> buildSslFactory(Optional<String> maybeTrustCaCerts,
                                                     Optional<String> maybeClientCerts,
                                                     Optional<String> maybeClientKey,
                                                     Optional<String> maybeKeyPassword) {
    Optional<X509ExtendedTrustManager> maybeTrustManager =
        maybeTrustCaCerts.map(trustedCaCertsLocation ->
            PemUtils.loadTrustMaterial(openStreamOrThrow(ResourceLocator.findNamedResource(trustedCaCertsLocation))));

    if (maybeClientCerts.isPresent() && !maybeClientKey.isPresent()) {
      throw new IllegalStateException("You provided a client cert, but not a client key. Cannot continue.");
    }
    if (maybeClientKey.isPresent() && !maybeClientCerts.isPresent()) {
      throw new IllegalStateException("You provided a client key, but not a client cert. Cannot continue.");
    }

    Optional<X509ExtendedKeyManager> maybeKeyManager =
        maybeClientCerts.flatMap(clientCertLocation ->
            maybeClientKey.map(clientKeyLocation -> {
              InputStream clientCertInputStream = openStreamOrThrow(ResourceLocator.findNamedResource(clientCertLocation));
              InputStream clientKeyInputStream = openStreamOrThrow(ResourceLocator.findNamedResource(clientKeyLocation));

              if (maybeKeyPassword.isPresent()) {
                return PemUtils.loadIdentityMaterial(clientCertInputStream, clientKeyInputStream, maybeKeyPassword.get().toCharArray());
              } else {
                return PemUtils.loadIdentityMaterial(clientCertInputStream, clientKeyInputStream);
              }
            }));

    SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();

    if (maybeKeyManager.isPresent()) {
      sslFactoryBuilder.withIdentityMaterial(maybeKeyManager.get());
      if (maybeTrustManager.isPresent()) {
        sslFactoryBuilder.withTrustMaterial(maybeTrustManager.get());
      } else {
        sslFactoryBuilder.withTrustMaterial(retrieveJavaDefaultTrustManager());
      }
      return Optional.of(sslFactoryBuilder.build());
    } else if (maybeTrustManager.isPresent()) {
      sslFactoryBuilder.withTrustMaterial(maybeTrustManager.get());
      return Optional.of(sslFactoryBuilder.build());
    }

    return Optional.empty();
  }

  private static X509TrustManager retrieveJavaDefaultTrustManager() {
    TrustManagerFactory trustManagerFactory;
    try {
      trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init((KeyStore) null);
      Optional<X509TrustManager> maybeDefaultJavaTrustManager =
          Arrays.stream(trustManagerFactory.getTrustManagers())
              .filter(trustManager -> trustManager instanceof X509TrustManager)
              .findFirst()
              .map(trustManager -> (X509TrustManager) trustManager);

      Preconditions.checkState(maybeDefaultJavaTrustManager.isPresent(),
          "The key manager is present, but the trusted ca certs setting is missing and a default trust manager could not be retrieved");

      return maybeDefaultJavaTrustManager.get();
    } catch (NoSuchAlgorithmException | KeyStoreException e) {
      throw new IllegalStateException("Unable to load the default java trust manager", e);
    }
  }

  private boolean isShutdown(OkHttpClient previousClient) {
    return DefaultHttpRequestReplyClientFactory.this.sharedClient != previousClient;
  }

  private static DefaultHttpRequestReplyClientSpec parseTransportProperties(ObjectNode transportClientProperties) {
    try {
      return DefaultHttpRequestReplyClientSpec.fromJson(OBJ_MAPPER, transportClientProperties);
    } catch (Exception e) {
      throw new RuntimeException("Unable to parse transport client properties when creating client: ", e);
    }
  }

  /** It's ok to use this InputStream without closing it since PemUtils do it for us */
  private static InputStream openStreamOrThrow(URL url) {
    try {
      return url.openStream();
    } catch (IOException e) {
      throw new IllegalStateException("Could not open " + url.getPath(), e);
    }
  }
}
