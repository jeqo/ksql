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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamWithHeaders;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public final class StreamSelectHeaderBuilder {

  private static final String EXP_TYPE = "SelectHeader";

  private StreamSelectHeaderBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamWithHeaders<K> selectHeaders,
      final KsqlQueryBuilder queryBuilder
  ) {
    final LogicalSchema sourceSchema = stream.getSchema();
    Map<ExpressionMetadata, Function<GenericRow, Object>> evaluators = new HashMap<>();

    for (Expression exp : selectHeaders.getHeaderExpressions()) {
      final ExpressionMetadata expression = buildExpressionEvaluator(
          exp,
          queryBuilder,
          sourceSchema
      );

      final ProcessingLogger processingLogger = queryBuilder
          .getProcessingLogger(selectHeaders.getProperties().getQueryContext());

      final String errorMsg = "Error extracting new key using expression " + exp;

      final Function<GenericRow, Object> evaluator = val -> expression
          .evaluate(val, null, processingLogger, () -> errorMsg);
      evaluators.put(expression, evaluator);
    }

    KStream<K, GenericRow> withHeader = stream.getStream()
        .transformValues(() -> new ValueTransformer<GenericRow, GenericRow>() {
          ProcessorContext processorContext;

          @Override
          public void init(ProcessorContext processorContext) {
            this.processorContext = processorContext;
          }

          @Override
          public GenericRow transform(GenericRow genericRow) {
            for (Map.Entry<ExpressionMetadata, Function<GenericRow, Object>> headerExp :
                evaluators.entrySet()) {
              processorContext.headers().add(
                  headerExp.getKey().arguments().get(0).name(),
                  headerExp.getValue().apply(genericRow).toString()
                      .getBytes(StandardCharsets.UTF_8));

            }
            return genericRow;
          }

          @Override
          public void close() {
          }
        });

    return stream.withStream(withHeader, sourceSchema);
  }

  private static <K> ExpressionMetadata buildExpressionEvaluator(
      final Expression expression,
      final KsqlQueryBuilder queryBuilder,
      final LogicalSchema sourceSchema
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(
        sourceSchema,
        queryBuilder.getKsqlConfig(),
        queryBuilder.getFunctionRegistry()
    );

    return codeGen.buildCodeGenFromParseTree(expression, EXP_TYPE);
  }
}
