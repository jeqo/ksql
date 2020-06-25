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

package io.confluent.ksql.execution.plan;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class StreamWithHeaders<K> implements ExecutionStep<KStreamHolder<K>> {

  private final ExecutionStepPropertiesV1 properties;
  private final ImmutableList<Expression> headerExpressions;
  @EffectivelyImmutable
  private final ExecutionStep<KStreamHolder<K>> source;

  public StreamWithHeaders(
      @JsonProperty(value = "properties", required = true) final ExecutionStepPropertiesV1 props,
      @JsonProperty(value = "source", required = true) final ExecutionStep<KStreamHolder<K>> source,
      @JsonProperty(value = "headerExpressions", required = true)
      final List<Expression> headerExpressions
  ) {
    this.properties = Objects.requireNonNull(props, "props");
    this.source = Objects.requireNonNull(source, "source");
    this.headerExpressions =
        ImmutableList.copyOf(Objects.requireNonNull(headerExpressions, "headerExpressions"));
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
    return properties;
  }

  @Override
  @JsonIgnore
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public List<Expression> getHeaderExpressions() {
    return headerExpressions;
  }

  public ExecutionStep<KStreamHolder<K>> getSource() {
    return source;
  }

  @Override
  public KStreamHolder<K> build(final PlanBuilder builder) {
    return builder.visitStreamSelectHeaders(this);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamWithHeaders<?> that = (StreamWithHeaders<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(headerExpressions, that.headerExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, source, headerExpressions);
  }
}
