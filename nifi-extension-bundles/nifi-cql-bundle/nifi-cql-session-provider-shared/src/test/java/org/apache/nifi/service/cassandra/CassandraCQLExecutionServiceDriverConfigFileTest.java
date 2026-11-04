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
package org.apache.nifi.service.cassandra;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.service.cql.api.service.AbstractCQLExecutionService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit coverage for {@code buildConfigLoader}, the composition behind the {@code Driver Configuration File}
 * property. The connection verification IT could only assert "a valid file still connects"; this pins the
 * behaviour a container never exercised - that an unset property is a pass-through, and that a set file is
 * layered ahead of the property-derived config so its options take effect (both an override and one the
 * file alone declares).
 */
class CassandraCQLExecutionServiceDriverConfigFileTest {

    private final CassandraCQLExecutionService service = new CassandraCQLExecutionService();

    @Test
    @DisplayName("With no Driver Configuration File set, the property-derived loader is returned unchanged")
    void testUnsetFileReturnsPropertyLoaderUnchanged() {
        final PropertyValue fileProperty = mock(PropertyValue.class);
        when(fileProperty.isSet()).thenReturn(false);

        final ConfigurationContext context = mock(ConfigurationContext.class);
        when(context.getProperty(AbstractCQLExecutionService.DRIVER_CONFIGURATION_FILE)).thenReturn(fileProperty);

        final DriverConfigLoader propertyLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(2))
                .build();

        assertSame(propertyLoader, service.buildConfigLoader(context, propertyLoader));
    }

    @Test
    @DisplayName("A Driver Configuration File is composed ahead of the property loader, so its options take effect")
    void testFileIsLayeredAheadOfPropertyLoader() throws IOException {
        final Path configFile = Files.createTempFile("CqlDriverConfig", ".conf");
        configFile.toFile().deleteOnExit();
        Files.writeString(configFile, """
                datastax-java-driver {
                  basic.request.timeout = 15 seconds
                  basic.request.page-size = 1234
                }
                """, StandardCharsets.UTF_8);

        final PropertyValue fileProperty = mock(PropertyValue.class);
        when(fileProperty.isSet()).thenReturn(true);
        when(fileProperty.evaluateAttributeExpressions()).thenReturn(fileProperty);
        when(fileProperty.getValue()).thenReturn(configFile.toString());

        final ConfigurationContext context = mock(ConfigurationContext.class);
        when(context.getProperty(AbstractCQLExecutionService.DRIVER_CONFIGURATION_FILE)).thenReturn(fileProperty);

        final DriverConfigLoader propertyLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(2))
                .build();

        final DriverExecutionProfile profile = service.buildConfigLoader(context, propertyLoader).getInitialConfig().getDefaultProfile();

        assertEquals(Duration.ofSeconds(15), profile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT),
                "the file's value must override the property-derived one");
        assertEquals(1234, profile.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE),
                "an option only the file declares must be in effect");
    }
}
