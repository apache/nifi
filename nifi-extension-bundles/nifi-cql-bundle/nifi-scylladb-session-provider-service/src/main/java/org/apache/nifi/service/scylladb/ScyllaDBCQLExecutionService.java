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

package org.apache.nifi.service.scylladb;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.service.cassandra.CassandraCQLExecutionService;

/**
 * The ScyllaDB session provider. Behaviourally this is the Cassandra service run against ScyllaDB's shard-aware
 * driver, which is why it has no body - the fork is supplied by this module's NAR rather than by this class.
 *
 * <p>The annotations below are not decoration. {@code @Tags} and {@code @CapabilityDescription} are
 * {@code @Inherited}, so without them this component reports the Cassandra service's documentation as its own: it
 * describes itself as being for Apache Cassandra, and carries no tag containing "scylla", which makes it unfindable
 * by name in the NiFi component picker. Any subclass of a documented component has to restate them.
 */
@Tags({"scylladb", "scylla", "cql", "cassandra", "database", "connection", "session", "pooling"})
@CapabilityDescription("Provides a CQL connection session for CQL processors and controller services to work with ScyllaDB, using ScyllaDB's "
        + "shard-aware driver. Use this service for ScyllaDB clusters; use CassandraCQLExecutionService for Apache "
        + "Cassandra.")
@SeeAlso(
        value = {CassandraCQLExecutionService.class},
        // Referenced by name rather than by class: a session provider has no business depending on the modules that
        // consume it, and nifi-cql-processors in particular could not accept the dependency - the ScyllaDB driver fork
        // this module carries is what its ban-database-client-dependencies enforcer rule exists to keep out.
        classNames = {
                "org.apache.nifi.processors.cql.PutCQLRecord",
                "org.apache.nifi.processors.cql.ExecuteCQLQueryRecord",
                "org.apache.nifi.service.cql.cache.CQLDistributedMapCache"
        })
public class ScyllaDBCQLExecutionService extends CassandraCQLExecutionService {
}
