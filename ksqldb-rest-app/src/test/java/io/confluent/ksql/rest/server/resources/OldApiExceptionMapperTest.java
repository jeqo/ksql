/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.resources;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.api.server.OldApiExceptionMapper;
import io.confluent.ksql.rest.EndpointResponse;
import io.confluent.ksql.rest.Errors;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import javax.ws.rs.WebApplicationException;
import org.junit.Test;

public class OldApiExceptionMapperTest {

  @Test
  public void shouldReturnEmbeddedResponseForKsqlRestException() {
    final EndpointResponse response = EndpointResponse.failed(400);
    assertThat(
        OldApiExceptionMapper.mapException(new KsqlRestException(response)),
        sameInstance(response));
  }

  @Test
  public void shouldReturnCorrectResponseForWebAppException() {
    final WebApplicationException webApplicationException = new WebApplicationException("error msg",
        403);
    final EndpointResponse response = OldApiExceptionMapper.mapException(webApplicationException);
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage) response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(40300));
    assertThat(response.getStatus(), equalTo(403));
  }

  @Test
  public void shouldReturnCorrectResponseForUnspecificException() {
    final EndpointResponse response = OldApiExceptionMapper
        .mapException(new Exception("error msg"));
    assertThat(response.getEntity(), instanceOf(KsqlErrorMessage.class));
    final KsqlErrorMessage errorMessage = (KsqlErrorMessage) response.getEntity();
    assertThat(errorMessage.getMessage(), equalTo("error msg"));
    assertThat(errorMessage.getErrorCode(), equalTo(Errors.ERROR_CODE_SERVER_ERROR));
    assertThat(response.getStatus(), equalTo(INTERNAL_SERVER_ERROR.code()));
  }
}
