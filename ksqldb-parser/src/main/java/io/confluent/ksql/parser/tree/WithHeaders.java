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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.util.KsqlException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class WithHeaders extends AstNode {

  private final ImmutableList<Expression> withHeaderExpressions;

  public WithHeaders(
      final Optional<NodeLocation> location,
      final List<Expression> withHeaderExpressions
  ) {
    super(location);
    this.withHeaderExpressions = ImmutableList
        .copyOf(requireNonNull(withHeaderExpressions, "withHeaderElements"));

    final HashSet<Object> withHeaders = new HashSet<>(withHeaderExpressions.size());

    if (withHeaderExpressions.isEmpty()) {
      throw new KsqlException("WITH HEADERS requires at least one expression");
    }

    withHeaderExpressions.forEach(exp -> {
      if (!withHeaders.add(exp)) {
        throw new KsqlException("Duplicate WITH HEADERS expression: " + exp);
      }
    });
  }

  public List<Expression> getWithHeaderExpressions() {
    return withHeaderExpressions;
  }

  @Override
  protected <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitWithHeaders(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final WithHeaders withHeaders = (WithHeaders) o;
    return Objects.equals(withHeaderExpressions, withHeaders.withHeaderExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(withHeaderExpressions);
  }

  @Override
  public String toString() {
    return "WithHeaders{"
        + "withHeaderExpressions=" + withHeaderExpressions
        + '}';
  }
}
