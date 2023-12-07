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

import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * Template for reading list of entities from database.
 *
 * @param <E> The entity type represented by a single database record.
 * @param <R> The result of the selection. Might be an aggregated value or collection.
 */
public class QuestDbEntityReadingTemplate<E, R> extends QuestDbReadingTemplate<R> {
    private final Function<Record, E> mapper;
    private final Function<List<E>, R> aggregator;

    /**
     * @param query The query to execute. Parameters might be added using the format specified by {@link String#format}.
     * @param mapper Responsible for mapping one database record into one entity object.
     * @param aggregator Might process the list of selected entities after the query has been executed.
     * @param errorResult Error handler in case of an exception arises during the execution.
     */
    public QuestDbEntityReadingTemplate(
            final String query,
            final Function<Record, E> mapper,
            final Function<List<E>, R> aggregator,
            final Function<Exception, R> errorResult) {
        super(query, errorResult);
        this.mapper = mapper;
        this.aggregator = aggregator;
    }

    @Override
    protected R processResult(final RecordCursor cursor) {
        final List<E> entities = new LinkedList<>();

        while (cursor.hasNext()) {
            entities.add(mapper.apply(cursor.getRecord()));
        }

        return aggregator.apply(entities);
    }
}
