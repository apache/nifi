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
package org.apache.nifi.hdfs.repository;

import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FAILURE_TIMEOUT_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.FULL_PERCENTAGE_PROPERTY;
import static org.apache.nifi.hdfs.repository.HdfsContentRepository.OPERATING_MODE_PROPERTY;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.config;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.prop;
import static org.apache.nifi.hdfs.repository.PropertiesBuilder.props;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.apache.nifi.util.NiFiProperties.DEFAULT_MAX_FLOWFILES_PER_CLAIM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class RepositoryConfigTest {

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Test
    public void defaultsTest() {
        RepositoryConfig config = new RepositoryConfig(NiFiProperties.createBasicNiFiProperties(null, null));
        assertEquals(0, config.getFailureTimeoutMs());
        assertEquals(0.95, config.getFullPercentage(), 0.001);
        assertEquals(1024 * 1024, config.getMaxAppendableClaimLength());
        assertEquals(1024, config.getSectionsPerContainer());
        assertEquals(Long.MAX_VALUE, config.getMaxArchiveMillis());
        assertEquals(Long.MAX_VALUE, config.getWaitForContainerTimeoutMs());
        assertEquals(0, config.getMaxArchiveRatio(), 0.001);
        assertEquals(DEFAULT_MAX_FLOWFILES_PER_CLAIM, config.getMaxFlowFilesPerClaim());
        assertFalse(config.isAlwaysSync());
        assertFalse(config.isArchiveData());
        assertEquals(set(OperatingMode.Normal), config.getModes());
    }

    @Test
    public void modesTest() {
        assertEquals(
            set(OperatingMode.Normal),
            config(props(prop(OPERATING_MODE_PROPERTY, "Normal"))).getModes()
        );

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.Archive),
            config(props(prop(OPERATING_MODE_PROPERTY, "Archive"))).getModes()
        );

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.Archive),
            config(props(prop(OPERATING_MODE_PROPERTY, "Normal,Archive"))).getModes()
        );

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.CapacityFallback),
            config(props(prop(OPERATING_MODE_PROPERTY, "CapacityFallback"))).getModes()
        );

        try {
            config(props(prop(OPERATING_MODE_PROPERTY, "FailureFallback")));
            fail("Config creation should have failed because no failure timeout is specified.");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("FailureFallback operating mode is active, and failure timeout is not specified."));
        }

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.FailureFallback),
            config(props(
                prop(OPERATING_MODE_PROPERTY, "FailureFallback"),
                prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
            )).getModes()
        );

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.CapacityFallback, OperatingMode.Archive),
            config(props(prop(OPERATING_MODE_PROPERTY, "CapacityFallback,Archive"))).getModes()
        );

        assertEquals(
            set(OperatingMode.Normal, OperatingMode.FailureFallback, OperatingMode.Archive),
            config(props(
                prop(OPERATING_MODE_PROPERTY, "FailureFallback,Archive"),
                prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
            )).getModes()
        );

        try {
            config(props(
                prop(OPERATING_MODE_PROPERTY, "FailureFallback,CapacityFallback"),
                prop(FAILURE_TIMEOUT_PROPERTY, "1 minute")
            ));
            fail("Config creation should have failed because both fallback methods are specified.");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("Both CapacityFallback and FailureFallback modes were specified in"));
        }
    }

    @Test
    public void failureTimeoutTest() {
        RepositoryConfig config = config(props(
            prop(FAILURE_TIMEOUT_PROPERTY, "5 minutes")
        ));

        final long FIVE_MINUTES = 1000 * 60 * 5L;
        assertEquals(FIVE_MINUTES, config.getFailureTimeoutMs());

        try {
            config(props(prop(FAILURE_TIMEOUT_PROPERTY, "5")));
            fail("Config creation should have failed because time property is in an invalid format");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("is not a valid time duration"));
        }
    }

    @Test
    public void percentagePropsTest() {
        RepositoryConfig config = config(props(
            prop(FULL_PERCENTAGE_PROPERTY, "55%"),
            prop(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, "77%")
        ));

        assertEquals(0.55, config.getFullPercentage(), 0.001);
        assertEquals(0.77, config.getMaxArchiveRatio(), 0.001);

        try {
            config(props(prop(FULL_PERCENTAGE_PROPERTY, "55")));
            fail("Config creation should have failed because percentage property is in an invalid format");
        } catch (RuntimeException ex) {
            assertTrue(ex.getMessage().contains("Value must be in format: <XX>%"));
        }
    }

    protected static Set<OperatingMode> set(OperatingMode ... modes) {
        Set<OperatingMode> set = new HashSet<>();
        for (OperatingMode mode : modes) {
            set.add(mode);
        }
        return set;
    }
}
