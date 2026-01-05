/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.services.dropbox;

import org.apache.nifi.util.MockPropertyConfiguration;
import org.apache.nifi.util.PropertyMigrationResult;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StandardDropboxCredentialServiceTest {
    @Test
    void testMigration() {
        final Map<String, String> propertyValues = Map.of(
                StandardDropboxCredentialService.ACCESS_TOKEN.getName(), "someAccessToken",
                StandardDropboxCredentialService.APP_KEY.getName(), "someAppKey",
                StandardDropboxCredentialService.APP_SECRET.getName(), "someAppSecret",
                StandardDropboxCredentialService.REFRESH_TOKEN.getName(), "someRefreshToken"
        );

        final MockPropertyConfiguration configuration = new MockPropertyConfiguration(propertyValues);
        final StandardDropboxCredentialService standardDropboxCredentialService = new StandardDropboxCredentialService();
        standardDropboxCredentialService.migrateProperties(configuration);

        Map<String, String> expected = Map.ofEntries(
                Map.entry("app-key", StandardDropboxCredentialService.APP_KEY.getName()),
                Map.entry("app-secret", StandardDropboxCredentialService.APP_SECRET.getName()),
                Map.entry("access-token", StandardDropboxCredentialService.ACCESS_TOKEN.getName()),
                Map.entry("refresh-token", StandardDropboxCredentialService.REFRESH_TOKEN.getName())
        );

        final PropertyMigrationResult result = configuration.toPropertyMigrationResult();
        final Map<String, String> propertiesRenamed = result.getPropertiesRenamed();

        assertEquals(expected, propertiesRenamed);
    }
}
