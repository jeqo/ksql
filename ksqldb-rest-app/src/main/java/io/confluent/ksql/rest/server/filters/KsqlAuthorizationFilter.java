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

package io.confluent.ksql.rest.server.filters;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.security.KsqlAuthorizationProvider;
import java.security.Principal;
import java.util.Set;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authorization filter for REST endpoints.
 */
@Priority(Priorities.AUTHORIZATION)
public class KsqlAuthorizationFilter implements ContainerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(KsqlAuthorizationFilter.class);

  private static final Set<String> PATHS_WITHOUT_AUTHORIZATION = ImmutableSet.of("/v1/metadata",
      "/v1/metadata/id", "/healthcheck");

  private final KsqlAuthorizationProvider authorizationProvider;

  public KsqlAuthorizationFilter(final KsqlAuthorizationProvider authorizationProvider) {
    this.authorizationProvider = authorizationProvider;
  }

  @Override
  public void filter(final ContainerRequestContext requestContext) {
    final Principal user = requestContext.getSecurityContext().getUserPrincipal();
    final String method = requestContext.getMethod(); // i.e GET, POST
    final String path = "/" + requestContext.getUriInfo().getPath();

    if (!requiresAuthorization(path)) {
      return;
    }

    try {
      authorizationProvider.checkEndpointAccess(user, method, path);
    } catch (final Throwable t) {
      log.warn(String.format("User:%s is denied access to \"%s %s\"",
          user.getName(), method, path), t);
      final EndpointResponse endpointResponse = Errors.accessDenied(t.getMessage());
      final Response response = Response.status(endpointResponse.getStatus())
          .entity(endpointResponse.getEntity()).build();
      requestContext.abortWith(response);
    }
  }

  public static Set<String> getPathsWithoutAuthorization() {
    return PATHS_WITHOUT_AUTHORIZATION;
  }

  private boolean requiresAuthorization(final String path) {
    return !PATHS_WITHOUT_AUTHORIZATION.contains(path);
  }

}
