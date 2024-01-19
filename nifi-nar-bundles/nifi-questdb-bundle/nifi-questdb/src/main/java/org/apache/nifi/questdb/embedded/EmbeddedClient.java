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
package org.apache.nifi.questdb.embedded;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoError;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlCompilerFactoryImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.mp.TimeoutBlockingWaitStrategy;
import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

final class EmbeddedClient implements Client {
    private final static Logger LOGGER = LoggerFactory.getLogger(EmbeddedClient.class);

    private final Supplier<CairoEngine> engine;
    private final AtomicBoolean disconnected = new AtomicBoolean(false);

    EmbeddedClient(final Supplier<CairoEngine> engine) {
        this.engine = engine;
    }

    @Override
    public void execute(final String query) throws DatabaseException {
        checkConnectionState();

        try (final SqlCompiler compiler = getCompiler()) {
            final CompiledQuery compile = compiler.compile(query, getSqlExecutionContext());
            compile.execute(new SCSequence(new TimeoutBlockingWaitStrategy(5, TimeUnit.SECONDS)));
        } catch (final SqlException | CairoError e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void insert(
        final String tableName,
        final InsertRowDataSource rowDataSource
    ) throws DatabaseException {
        checkConnectionState();

        if (!rowDataSource.hasNextToInsert()) {
            LOGGER.debug("No rows to insert into {}", tableName);
            return;
        }

        final TableToken tableToken = engine.get().getTableTokenIfExists(tableName);

        if (tableToken == null) {
            throw new DatabaseException(String.format("Table Token for table [%s] not found", tableName));
        }

        try (
            final TableWriter tableWriter = engine.get().getWriter(tableToken, "adding rows")
        ) {
            final TableWriterBasedInsertRowContext context = new TableWriterBasedInsertRowContext(tableWriter);

            while (rowDataSource.hasNextToInsert()) {
                context.addRow(rowDataSource);
            }

            LOGGER.debug("Committing {} rows", tableWriter.getRowCount());
            tableWriter.commit();
        } catch (final Exception | CairoError e) {
            // CairoError might be thrown in extreme cases, for example when no space left on the disk
            throw new DatabaseException(e);
        } finally {
            engine.get().releaseInactive();
        }
    }

    @Override
    public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
        checkConnectionState();

        final CompiledQuery compiledQuery;

        try (final SqlCompiler compiler = getCompiler()) {
            compiledQuery = compiler.compile(query, getSqlExecutionContext());
        } catch (final SqlException | CairoError e) {
            throw new DatabaseException(e);
        }

        try (
            final RecordCursorFactory factory = compiledQuery.getRecordCursorFactory();
            final RecordCursor cursor = factory.getCursor(getSqlExecutionContext());
        ) {
            final CursorBasedQueryRowContext rowContext = new CursorBasedQueryRowContext(cursor);

            while ((rowContext.hasNext())) {
                rowContext.moveToNext();
                rowProcessor.processRow(rowContext);
            }

            return rowProcessor.getResult();
        } catch (final Exception e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void disconnect() throws DatabaseException {
        checkConnectionState();
        disconnected.set(true);
        LOGGER.info("Client disconnected");
    }

    private void checkConnectionState() throws DatabaseException {
        if (disconnected.get()) {
            throw new ClientDisconnectedException("The client is already disconnected");
        }
    }

    private SqlCompiler getCompiler() {
        return SqlCompilerFactoryImpl.INSTANCE.getInstance(engine.get());
    }

    private SqlExecutionContext getSqlExecutionContext() {
        return SqlExecutionContextFactory.getInstance(engine.get());
    }
}
