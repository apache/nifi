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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.sql.ExecuteSQLCommonAttributes;
import org.apache.nifi.processors.standard.sql.ExecuteSQLCommonProperties;
import org.apache.nifi.processors.standard.sql.ExecuteSQLCommonRelationships;
import org.apache.nifi.processors.standard.sql.ExecuteSQLConfiguration;
import org.apache.nifi.processors.standard.sql.ExecuteSQLFetchSession;
import org.apache.nifi.processors.standard.sql.ResultSetFragments;
import org.apache.nifi.processors.standard.sql.SqlWriter;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;


public abstract class AbstractExecuteSQL extends AbstractSessionFactoryProcessor
        implements ExecuteSQLCommonProperties, ExecuteSQLCommonRelationships, ExecuteSQLCommonAttributes {

    protected Set<Relationship> relationships;
    protected List<PropertyDescriptor> propDescriptors;

    protected DBCPService dbcpService;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        config.renameProperty("SQL select query", SQL_QUERY.getName());
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) throws ProcessException {
        final ProcessSession session = sessionFactory.createSession();
        try {
            FlowFile fileToProcess = null;
            if (context.hasIncomingConnection()) {
                fileToProcess = session.get();

                // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
                // However, if we have no FlowFile and we have connections coming from other Processors, then
                // we know that we should run only if we have a FlowFile.
                if (fileToProcess == null && context.hasNonLoopConnection()) {
                    return;
                }
            }

            final ComponentLog logger = getLogger();
            final ExecuteSQLConfiguration config = new ExecuteSQLConfiguration(context, session, fileToProcess, this::getQueries);
            final SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);

            final ResultSetFragments resultSetFragments = new ResultSetFragments(session, config, fileToProcess, sessionFactory);
            final Runnable fetchSession = new ExecuteSQLFetchSession(context, session, config, dbcpService, logger, sqlWriter, resultSetFragments);
            fetchSession.run();
            session.commitAsync();
        } catch (final Throwable t) {
            session.rollback(true);
            throw t;
        }
    }

    /*
     * Extract list of queries from config property
     */
    protected List<String> getQueries(final String value) {
        if (value == null || value.isEmpty() || value.isBlank()) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split("(?<!\\\\);")) {
            query = query.replaceAll("\\\\;", ";");
            if (!query.isBlank()) {
                queries.add(query.trim());
            }
        }
        return queries;
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess);

}
