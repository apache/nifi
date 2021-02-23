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
import io.questdb.cairo.TableWriter;
import io.questdb.griffin.SqlExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Template for writing entries into QuestDb.
 *
 * @param <T> The type of the entry.
 */
public abstract class QuestDbWritingTemplate<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuestDbWritingTemplate.class);

    private final String tableName;

    /**
     * @param tableName Name of the target table.
     */
    protected QuestDbWritingTemplate(final String tableName) {
        this.tableName = tableName;
    }

    /**
     * Inserts the entries into the database.
     *
     * @param engine QuestDB engine.
     * @param context Execution context.
     * @param entries Entries to insert.
     */
    public void insert(final CairoEngine engine, final SqlExecutionContext context, final Collection<T> entries) {
        if (entries.isEmpty()) {
            return;
        }

        try (
            final TableWriter tableWriter = engine.getWriter(context.getCairoSecurityContext(), tableName);
        ) {
            addRows(tableWriter, entries);
            tableWriter.commit();
        } catch (final Exception e) {
            LOGGER.error("Error happened during writing into table " + tableName, e);
        } finally {
            engine.releaseInactive();
        }
    }

    /**
     * Populating {@link TableWriter} with data extracted from entries.
     *
     * @param tableWriter Table writer.
     * @param entries List of entries.
     */
    abstract protected void addRows(TableWriter tableWriter, Collection<T> entries);
}
