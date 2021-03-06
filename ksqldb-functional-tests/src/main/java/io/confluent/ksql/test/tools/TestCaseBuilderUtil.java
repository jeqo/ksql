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

package io.confluent.ksql.test.tools;

import static com.google.common.io.Files.getNameWithoutExtension;

import com.google.common.collect.Streams;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.KsqlParser.ParsedStatement;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.SqlBaseParser;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElement.Namespace;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.topic.TopicFactory;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Util class for common code used by test case builders
 */
@SuppressWarnings("UnstableApiUsage")
public final class TestCaseBuilderUtil {

  private static final String FORMAT_REPLACE_ERROR =
      "To use {FORMAT} in your statements please set the 'format' test case element";

  private TestCaseBuilderUtil() {
  }

  public static String buildTestName(
      final Path testPath,
      final String testName,
      final Optional<String> explicitFormat
  ) {
    final String prefix = filePrefix(testPath.toString());

    final String pf = explicitFormat
        .map(f -> " - " + f)
        .orElse("");

    return prefix + testName + pf;
  }

  public static String extractSimpleTestName(
      final String testPath,
      final String testName
  ) {
    final String prefix = filePrefix(testPath);

    if (!testName.startsWith(prefix)) {
      throw new IllegalArgumentException("Not prefixed test name: " + testName);
    }

    return testName.substring(prefix.length());
  }

  public static List<String> buildStatements(
      final List<String> statements,
      final Optional<String> explicitFormat
  ) {
    final String format = explicitFormat.orElse(FORMAT_REPLACE_ERROR);

    return statements.stream()
        .map(stmt -> stmt.replace("{FORMAT}", format))
        .collect(Collectors.toList());
  }

  public static Collection<Topic> getAllTopics(
      final Collection<String> statements,
      final Collection<Topic> topics,
      final Collection<Record> outputs,
      final Collection<Record> inputs,
      final FunctionRegistry functionRegistry
  ) {
    final Map<String, Topic> allTopics = new HashMap<>();

    // Add all topics from topic nodes to the map:
    topics.forEach(topic -> allTopics.put(topic.getName(), topic));

    // Infer topics if not added already:
    statements.stream()
        .map(sql -> createTopicFromStatement(sql, functionRegistry))
        .filter(Objects::nonNull)
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

    // Get topics from inputs and outputs fields:
    Streams.concat(inputs.stream(), outputs.stream())
        .map(record -> new Topic(record.getTopicName(), Optional.empty()))
        .forEach(topic -> allTopics.putIfAbsent(topic.getName(), topic));

    return allTopics.values();
  }

  private static Topic createTopicFromStatement(
      final String sql,
      final FunctionRegistry functionRegistry
  ) {
    final KsqlParser parser = new DefaultKsqlParser();
    final MetaStoreImpl metaStore = new MetaStoreImpl(functionRegistry);

    final Predicate<ParsedStatement> onlyCSandCT = stmt ->
        stmt.getStatement().statement() instanceof SqlBaseParser.CreateStreamContext
            || stmt.getStatement().statement() instanceof SqlBaseParser.CreateTableContext;

    final Function<PreparedStatement<?>, Topic> extractTopic = (PreparedStatement<?> stmt) -> {
      final CreateSource statement = (CreateSource) stmt.getStatement();

      final KsqlTopic ksqlTopic = TopicFactory.create(statement.getProperties());

      final ValueFormat valueFormat = ksqlTopic.getValueFormat();
      final Optional<ParsedSchema> valueSchema;
      if (valueFormat.getFormat().supportsSchemaInference()) {
        final List<SimpleColumn> valueColumns = statement.getElements().stream()
            .filter(e -> e.getNamespace() == Namespace.VALUE)
            .map(e -> new TestColumn(e.getName(), e.getType().getSqlType()))
            .collect(Collectors.toList());

        final FormatInfo formatInfo = valueFormat.getFormatInfo();

        valueSchema = valueColumns.isEmpty()
            ? Optional.empty()
            : Optional.of(valueFormat.getFormat().toParsedSchema(valueColumns, formatInfo));
      } else {
        valueSchema = Optional.empty();
      }

      return new Topic(ksqlTopic.getKafkaTopicName(), valueSchema);
    };

    try {
      final List<ParsedStatement> parsed = parser.parse(sql);
      if (parsed.size() > 1) {
        throw new IllegalArgumentException("SQL contains more than one statement: " + sql);
      }

      final List<Topic> topics = parsed.stream()
          .filter(onlyCSandCT)
          .map(stmt -> parser.prepare(stmt, metaStore))
          .map(extractTopic)
          .collect(Collectors.toList());

      return topics.isEmpty() ? null : topics.get(0);
    } catch (final Exception e) {
      // Statement won't parse: this will be detected/handled later.
      System.out.println("Error parsing statement (which may be expected): " + sql);
      e.printStackTrace(System.out);
      return null;
    }
  }

  private static String filePrefix(final String testPath) {
    return getNameWithoutExtension(testPath) + " - ";
  }

  private static class TestColumn implements SimpleColumn {

    private final ColumnName name;
    private final SqlType type;

    TestColumn(final ColumnName name, final SqlType type) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public ColumnName name() {
      return name;
    }

    @Override
    public SqlType type() {
      return type;
    }
  }
}
