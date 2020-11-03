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

package io.confluent.ksql.execution.plan;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public final class GlobalKTableHolder<K> {

  private final GlobalKTable<K, GenericRow> stream;
  private final KeySerdeFactory<K> keySerdeFactory;
  private final LogicalSchema schema;
  @EffectivelyImmutable // Ignored
  private final Optional<MaterializationInfo.Builder> materializationBuilder;

  private GlobalKTableHolder(
      final GlobalKTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory,
      final Optional<MaterializationInfo.Builder> materializationBuilder
  ) {
    this.stream = Objects.requireNonNull(stream, "stream");
    this.keySerdeFactory = Objects.requireNonNull(keySerdeFactory, "keySerdeFactory");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.materializationBuilder =
        Objects.requireNonNull(materializationBuilder, "materializationProvider");
  }

  public static <K> GlobalKTableHolder<K> unmaterialized(
      final GlobalKTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory
  ) {
    return new GlobalKTableHolder<>(stream, schema, keySerdeFactory, Optional.empty());
  }

  public static <K> GlobalKTableHolder<K> materialized(
      final GlobalKTable<K, GenericRow> stream,
      final LogicalSchema schema,
      final KeySerdeFactory<K> keySerdeFactory,
      final MaterializationInfo.Builder materializationBuilder
  ) {
    return new GlobalKTableHolder<>(
        stream,
        schema,
        keySerdeFactory,
        Optional.of(materializationBuilder)
    );
  }

  public KeySerdeFactory<K> getKeySerdeFactory() {
    return keySerdeFactory;
  }

  public GlobalKTable<K, GenericRow> getTable() {
    return stream;
  }

  public Optional<MaterializationInfo.Builder> getMaterializationBuilder() {
    return materializationBuilder;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public GlobalKTableHolder<K> withTable(final GlobalKTable<K, GenericRow> table, final LogicalSchema schema) {
    return new GlobalKTableHolder<>(table, schema, keySerdeFactory, materializationBuilder);
  }

  public GlobalKTableHolder<K> withMaterialization(final Optional<MaterializationInfo.Builder> builder) {
    return new GlobalKTableHolder<>(
        stream,
        schema,
        keySerdeFactory,
        builder
    );
  }
}