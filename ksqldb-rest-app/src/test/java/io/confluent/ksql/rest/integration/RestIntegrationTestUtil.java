/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.integration;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.UrlEscapers;
import io.confluent.ksql.rest.ApiJsonMapper;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.client.KsqlRestClient;
import io.confluent.ksql.rest.client.RestResponse;
import io.confluent.ksql.rest.entity.CommandStatus;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.CommandStatuses;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.entity.KsqlRequest;
import io.confluent.ksql.rest.entity.ServerClusterId;
import io.confluent.ksql.rest.entity.ServerInfo;
import io.confluent.ksql.rest.entity.ServerMetadata;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.test.util.secure.Credentials;
import io.confluent.ksql.util.TestDataProvider;
import io.confluent.ksql.util.VertxCompletableFuture;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpVersion;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;

public final class RestIntegrationTestUtil {

  private RestIntegrationTestUtil() {
  }

  public static List<KsqlEntity> makeKsqlRequest(final TestKsqlRestApp restApp, final String sql) {
    return makeKsqlRequest(restApp, sql, Optional.empty());
  }

  static List<KsqlEntity> makeKsqlRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

      throwOnError(res);

      return awaitResults(restClient, res.getResponse());
    }
  }

  static KsqlErrorMessage makeKsqlRequestWithError(
      final TestKsqlRestApp restApp,
      final String sql
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<KsqlEntityList> res = restClient.makeKsqlRequest(sql);

      throwOnNoError(res);

      return res.getErrorMessage();
    }
  }

  static ServerInfo makeInfoRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerInfo> res = restClient.getServerInfo();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static CommandStatus makeStatusRequest(final TestKsqlRestApp restApp, final String commandId) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<CommandStatus> res = restClient.getStatus(commandId);

      throwOnError(res);

      return res.getResponse();
    }
  }

  static CommandStatuses makeStatusesRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<CommandStatuses> res = restClient.getAllStatuses();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static ServerMetadata makeServerMetadataRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerMetadata> res = restClient.getServerMetadata();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static ServerClusterId makeServerMetadataIdRequest(final TestKsqlRestApp restApp) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(Optional.empty())) {

      final RestResponse<ServerClusterId> res = restClient.getServerMetadataId();

      throwOnError(res);

      return res.getResponse();
    }
  }

  static List<StreamedRow> makeQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds
  ) {
    return makeQueryRequest(restApp, sql, userCreds, null);
  }

  static List<StreamedRow> makeQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds,
      final Map<String, ?> properties
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<List<StreamedRow>> res =
          restClient.makeQueryRequest(sql, null, properties);

      throwOnError(res);

      return res.getResponse();
    }
  }

  static KsqlErrorMessage makeQueryRequestWithError(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<BasicCredentials> userCreds,
      final Map<String, ?> properties
  ) {
    try (final KsqlRestClient restClient = restApp.buildKsqlClient(userCreds)) {

      final RestResponse<List<StreamedRow>> res =
          restClient.makeQueryRequest(sql, null, properties);

      throwOnNoError(res);

      return res.getErrorMessage();
    }
  }

  /**
   * Make a query request using a basic Http client.
   *
   * @param restApp the test app instance to issue the request against
   * @param sql the sql payload
   * @param cmdSeqNum optional sequence number of previous command
   * @return the response payload
   */
  static String rawRestQueryRequest(
      final TestKsqlRestApp restApp,
      final String sql,
      final Optional<Long> cmdSeqNum
  ) {

    final KsqlRequest request = new KsqlRequest(
        sql,
        ImmutableMap.of(),
        Collections.emptyMap(),
        cmdSeqNum.orElse(null)
    );
    return rawRestRequest(restApp, HttpVersion.HTTP_1_1, HttpMethod.POST, "/query", request).body()
        .toString();
  }

  static HttpResponse<Buffer> rawRestRequest(
      final TestKsqlRestApp restApp,
      final HttpVersion httpVersion,
      final HttpMethod method,
      final String uri,
      final Object requestBody
  ) {

    Vertx vertx = Vertx.vertx();
    WebClient webClient = null;

    try {
      WebClientOptions webClientOptions = new WebClientOptions()
          .setDefaultHost(restApp.getHttpListener().getHost())
          .setDefaultPort(restApp.getHttpListener().getPort())
          .setFollowRedirects(false);

      if (httpVersion == HttpVersion.HTTP_2) {
        webClientOptions.setProtocolVersion(HttpVersion.HTTP_2).setHttp2ClearTextUpgrade(false);
      }

      webClient = WebClient.create(vertx, webClientOptions);

      byte[] bytes = ApiJsonMapper.INSTANCE.get().writeValueAsBytes(requestBody);

      Buffer bodyBuffer = Buffer.buffer(bytes);

      // When:
      VertxCompletableFuture<HttpResponse<Buffer>> requestFuture = new VertxCompletableFuture<>();
      HttpRequest<Buffer> request = webClient
          .request(method, uri)
          .putHeader("content-type", "application/json");
      if (bodyBuffer != null) {
        request.sendBuffer(bodyBuffer, requestFuture);
      } else {
        request.send(requestFuture);
      }
      return requestFuture.get();

    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      if (webClient != null) {
        webClient.close();
      }
      vertx.close();
    }
  }

  public static void createStream(final TestKsqlRestApp restApp,
      final TestDataProvider<?> dataProvider) {
    makeKsqlRequest(
        restApp,
        "CREATE STREAM " + dataProvider.kstreamName()
            + " (" + dataProvider.ksqlSchemaString(false) + ") "
            + "WITH (kafka_topic='" + dataProvider.topicName() + "', value_format='json');"
    );
  }

  private static List<KsqlEntity> awaitResults(
      final KsqlRestClient ksqlRestClient,
      final List<KsqlEntity> pending
  ) {
    return pending.stream()
        .map(e -> awaitResult(e, ksqlRestClient))
        .collect(Collectors.toList());
  }

  private static KsqlEntity awaitResult(
      final KsqlEntity e,
      final KsqlRestClient ksqlRestClient
  ) {
    if (!(e instanceof CommandStatusEntity)) {
      return e;
    }

    CommandStatusEntity cse = (CommandStatusEntity) e;
    final String commandId = cse.getCommandId().toString();

    while (cse.getCommandStatus().getStatus() != Status.ERROR
        && cse.getCommandStatus().getStatus() != Status.SUCCESS) {

      final RestResponse<CommandStatus> res = ksqlRestClient.makeStatusRequest(commandId);

      throwOnError(res);

      cse = new CommandStatusEntity(
          cse.getStatementText(),
          cse.getCommandId(),
          res.getResponse(),
          cse.getCommandSequenceNumber()
      );
    }

    return cse;
  }

  private static void throwOnError(final RestResponse<?> res) {
    if (res.isErroneous()) {
      throw new AssertionError("Failed to await result."
          + "msg: " + res.getErrorMessage());
    }
  }

  private static void throwOnNoError(final RestResponse<?> res) {
    if (!res.isErroneous()) {
      throw new AssertionError("Failed to get erroneous result.");
    }
  }

  static WebSocketClient makeWsRequest(
      final URI baseUri,
      final String sql,
      final Object listener,
      final Optional<MediaType> mediaType,
      final Optional<MediaType> contentType,
      final Optional<Credentials> credentials
  ) {
    try {
      final WebSocketClient wsClient = new WebSocketClient();
      wsClient.start();

      final ClientUpgradeRequest request = new ClientUpgradeRequest();

      credentials.ifPresent(creds -> request
          .setHeader(HttpHeaders.AUTHORIZATION, "Basic " + buildBasicAuthHeader(creds)));

      mediaType.ifPresent(mt -> request.setHeader(HttpHeaders.ACCEPT, mt.toString()));
      contentType.ifPresent(ct -> request.setHeader(HttpHeaders.CONTENT_TYPE, ct.toString()));

      final URI wsUri = baseUri.resolve("/ws/query?request=" + buildStreamingRequest(sql));

      wsClient.connect(listener, wsUri, request);

      return wsClient;
    } catch (final Exception e) {
      throw new RuntimeException("failed to create ws client", e);
    }
  }

  private static String buildBasicAuthHeader(final Credentials credentials) {
    final String creds = credentials.username + ":" + credentials.password;
    return Base64.getEncoder().encodeToString(creds.getBytes(Charset.defaultCharset()));
  }

  private static String buildStreamingRequest(final String sql) {
    return UrlEscapers.urlFormParameterEscaper()
        .escape("{"
            + " \"ksql\": \"" + sql + "\""
            + "}");
  }
}
