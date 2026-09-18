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
package org.apache.nifi.service.cql.it;

import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an integration suite as testing how a connection is <em>established</em> - authentication, TLS, and
 * {@code verify()} - rather than what can be done once one exists. Suites so marked run only when
 * {@value #ENABLED_PROPERTY} is set to {@code true}.
 *
 * <p>These are the expensive suites and the ones least likely to be affected by a change to query or write
 * behaviour: authentication and TLS each provision a dedicated container per scenario, because the server
 * settings they exercise are baked into {@code cassandra.yaml}/{@code scylla.yaml} at startup and cannot be
 * toggled on a running one. Skipping them by default keeps the routine integration run to the suites that
 * exercise the code most changes actually touch.
 *
 * <p>Enable with:
 * <pre>{@code mvn verify -Pintegration-tests -DTEST_CQL_CONNECTION_TESTS=true}</pre>
 *
 * <p>The condition is applied at class level, so a disabled suite never reaches {@code @BeforeAll} (or, on a
 * {@code @ParameterizedClass}, {@code @BeforeParameterizedClassInvocation}) and therefore never starts a
 * container. Being skipped rather than excluded also means the run still reports that they did not execute,
 * instead of quietly showing a smaller test count.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
@EnabledIfSystemProperty(named = ConnectionTest.ENABLED_PROPERTY, matches = "(?i)true")
public @interface ConnectionTest {

    /**
     * The system property that opts these suites in. Named to match the {@code TEST_*} convention the other
     * integration-test switches in this bundle already use ({@code TEST_CASSANDRA_OLDER_VERSIONS},
     * {@code TEST_SCYLLA_DDL_TIMEOUT_SECONDS}).
     */
    String ENABLED_PROPERTY = "TEST_CQL_CONNECTION_TESTS";
}
