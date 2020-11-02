/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.query.QueryId;
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class QueryControlStatement extends Statement {

  public static final String ALL_QUERIES = "ALL";
  final Optional<QueryId> queryId;

  QueryControlStatement(final Optional<NodeLocation> location, final Optional<QueryId> queryId) {
    super(location);
    this.queryId = Objects.requireNonNull(queryId, "queryId");
  }

  /**
   * @return the id of the query to terminate or {@code empty()} if all should be terminated.
   */
  public Optional<QueryId> getQueryId() {
    return queryId;
  }
}
