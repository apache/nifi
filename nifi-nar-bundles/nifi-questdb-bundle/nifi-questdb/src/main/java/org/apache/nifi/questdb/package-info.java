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

/**
 * <p>
 *     This package wraps all QuestDB features utilized by NiFi. The content of the package serves as an adaptor and a facade
 *     for QuestDB API, encapsulating all the direct dependencies for QuestDB.
 * </p>
 *
 * <p>
 *     This has multiple purposes such as:
 * </p>
 *
 * <ul>
 *     <li>Making version changes of the underlying library easier.</li>
 *     <li>Preventing dependency sprawl.</li>
 *     <li>Hiding usage details.</li>
 * </ul>
 *
 * <p>
 *     The main entry point for the <code>DatabaseManager</code> which is responsible for returning a <code>Client</code> which
 *     might be used for the NiFi code to interact with the QuestDB instance.
 * </p>
 *
 * <h3>Embedded implementation</h3>
 *
 * <p>
 *     Currently the bundle supports connecting to an embedded QuestDB instance which is managed by the <code>EmbeddedDatabaseManager</code>.
 *     This includes creating the database and necessary tables and also rolling over old data. In order to up an embedded database a
 *     properly parametrized <code>EmbeddedDatabaseManagerContext</code> is necessary with the <code>ManagedTableDefinition</code>
 *     instances in place.
 * </p>
 *
 * <p>
 *     Users can specify table definition and roll over strategy via <code>ManagedTableDefinition</code> instances. The manager then
 *     will ensure that the tables are created and not altered. It is not recommended to execute alterations on the tables from outside
 *     sources. Version management for table schemes is not currently supported.
 * </p>
 *
 *

 * <h3>Mapping</h3>
 *
 * <p>
 *     For ease of use, the bundle supports "mapping" between "rows" (representation of data within QuestDB) and "entries" (representation
 *     of data within NiFi) when executing queries. In order to utilize this capability a declerative description of the mapping is needed
 *     using the <code>RequestMappingBuilder</code>. It can be wrapped by <code>RequestMapping#getResultProcessor</code> and used as any
 *     <code>QueryResultProcessor</code>.
 * </p>
 *
 * <h3>Error handling</h3>
 *
 * <p>
 *     The bundle provides error handling using various exceptions all derived from <code>DatabaseException</code>. The embedded implementation
 *     also provides a level of "restoration" capabilities in case the situation allows it. This can lead to situations where the existing database
 *     is moved out from it's place and a new instance is created! Also: in extreme cases (especially with storage related issues) the corrupted
 *     database instance might be deleted and/or the service falls back to a dummy mode, similar to a circuit breaker. Resolving these situations
 *     might need human intervention.
 * </p>
 */
package org.apache.nifi.questdb;
