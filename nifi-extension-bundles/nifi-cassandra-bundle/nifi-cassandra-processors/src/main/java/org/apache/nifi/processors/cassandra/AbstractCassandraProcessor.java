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
package org.apache.nifi.processors.cassandra;

import org.apache.nifi.cassandra.CassandraConnectionService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.FlowFileHandlingException;

import java.util.List;

/**
 * AbstractCassandraProcessor is a base class for Cassandra processors and contains logic and variables common to most
 * processors integrating with Apache Cassandra.
 */
public abstract class AbstractCassandraProcessor extends AbstractProcessor {

    static final PropertyDescriptor CASSANDRA_CONNECTION_PROVIDER = new PropertyDescriptor.Builder()
            .name("Cassandra Connection Provider")
            .description("""
                    Specifies the Cassandra connection providing controller service to be used to connect to
                    Cassandra cluster.
                    """)
            .required(true)
            .identifiesControllerService(CassandraConnectionService.class)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is transferred to this relationship if the operation completed successfully.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is transferred to this relationship if the operation failed.")
            .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("""
                    A FlowFile is transferred to this relationship if the operation cannot be completed but attempting
                    it again may succeed.
                    """)
            .build();

    protected static final List<PropertyDescriptor> COMMON_PROPERTY_DESCRIPTORS = List.of(
            CASSANDRA_CONNECTION_PROVIDER
    );

    protected CassandraConnectionService getConnectionService(final ProcessContext context) {
        return context.getProperty(CASSANDRA_CONNECTION_PROVIDER)
                .asControllerService(CassandraConnectionService.class);
    }

    protected String createTransitUri(CassandraConnectionService service, String query) {
        String location = service.getDatabaseLocation();
        return "cassandra://" + location + "/" + query;
    }

    protected void routeOnError(final ProcessContext context, final ProcessSession session,
                              final FlowFile incomingFlowFile, final Relationship relationship) {
        if (context.hasIncomingConnection()) {
            try {
                final FlowFile toRoute = incomingFlowFile != null ? incomingFlowFile : session.create();
                session.transfer(session.penalize(toRoute), relationship);
            } catch (FlowFileHandlingException e) {
                FlowFile proxy = session.create();
                if (incomingFlowFile != null) {
                    proxy = session.putAllAttributes(proxy, incomingFlowFile.getAttributes());
                }
                session.transfer(session.penalize(proxy), relationship);
            }
        } else {
            context.yield();
        }
    }
}

