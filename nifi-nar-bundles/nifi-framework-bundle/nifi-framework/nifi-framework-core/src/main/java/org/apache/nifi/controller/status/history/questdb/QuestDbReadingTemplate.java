/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.status.history.questdb;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Function;

/**
 * Template class for executing selection from QuestDB. The {@code readEntities} might be called multiple times, it will
 * execute the query multiple times independently, always with the given parameters.
 *
 * @param <R> The result of the selection. Might be an aggregated value or collection.
 */
abstract public class QuestDbReadingTemplate<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuestDbReadingTemplate.class);

    private final String query;
    private final Function<Exception, R> errorResult;

    /**
     * @param query The query to execute. Parameters might be added using the format specified by {@link String#format}.
     * @param errorResult Error handler in case of an exception arises during the execution.
     */
    public QuestDbReadingTemplate(
            final String query,
            final Function<Exception, R> errorResult) {
        this.query = query;
        this.errorResult = errorResult;
    }

    /**
     * Executes the query and returns with the result.
     *
     * @param engine The database engine.
     * @param context The execution context.
     * @param parameters Parameters (is any) in the order of appearance in the query string.
     *
     * @return End result of the query, after possible procession by {@link #processResult(RecordCursor)}
     */
    public R read(final CairoEngine engine, final SqlExecutionContext context, final List<Object> parameters) {
        try (
            final SqlCompiler compiler = new SqlCompiler(engine);
            final RecordCursorFactory factory = compiler.compile(String.format(query, parameters.toArray()), context).getRecordCursorFactory();
            final RecordCursor cursor = factory.getCursor(context);
        ) {
            return processResult(cursor);
        } catch (final Exception e) {
            LOGGER.error("Error during reading from database", e);
            return errorResult.apply(e);
        }
    }

    protected abstract R processResult(RecordCursor cursor);
}
