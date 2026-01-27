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
package org.apache.nifi.processors.snowflake;

import org.apache.nifi.processors.snowflake.util.SnowflakeProperties;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPutSnowflakeInternalStage {
    @Test
    void testMigrateProperties() {
        final TestRunner testRunner = TestRunners.newTestRunner(PutSnowflakeInternalStage.class);
        final Map<String, String> expectedRenamed = Map.ofEntries(
                Map.entry("snowflake-connection-provider", PutSnowflakeInternalStage.SNOWFLAKE_CONNECTION_PROVIDER.getName()),
                Map.entry("internal-stage-type", PutSnowflakeInternalStage.INTERNAL_STAGE_TYPE.getName()),
                Map.entry("table", PutSnowflakeInternalStage.TABLE.getName()),
                Map.entry("internal-stage", PutSnowflakeInternalStage.INTERNAL_STAGE.getName()),
                Map.entry(SnowflakeProperties.OLD_DATABASE_PROPERTY_NAME, SnowflakeProperties.DATABASE.getName()),
                Map.entry(SnowflakeProperties.OLD_SCHEMA_PROPERTY_NAME, SnowflakeProperties.SCHEMA.getName())
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
