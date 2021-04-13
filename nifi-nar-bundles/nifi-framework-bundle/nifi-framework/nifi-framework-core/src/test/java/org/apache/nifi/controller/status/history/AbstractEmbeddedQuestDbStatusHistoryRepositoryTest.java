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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.util.FileUtils;
import org.apache.nifi.util.NiFiProperties;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public abstract class AbstractEmbeddedQuestDbStatusHistoryRepositoryTest extends AbstractStatusHistoryRepositoryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEmbeddedQuestDbStatusHistoryRepositoryTest.class);

    protected static final String PATH = "target/questdb";
    protected static final long NOW = System.currentTimeMillis();
    protected static final Date START = new Date(0);
    protected static final Date INSERTED_AT = new Date(NOW - TimeUnit.MINUTES.toMillis(1));
    protected static final Date END = new Date(NOW);
    protected static final Date END_EARLY = new Date(NOW - TimeUnit.MINUTES.toMillis(10));

    protected static final int PREFERRED_DATA_POINTS = 1000;
    protected static final int DAYS_TO_KEEP_DATA = 7;
    protected static final long PERSIST_FREQUENCY = 50; //200 milliseconds

    protected EmbeddedQuestDbStatusHistoryRepository testSubject;
    protected String path;

    @Before
    public void setUp() throws Exception {
        path = PATH + System.currentTimeMillis();
        testSubject = givenTestSubject();
    }

    @After
    public void tearDown() throws Exception {
        testSubject.shutdown();

        try {
            FileUtils.deleteFile(new File(path), true);
        } catch (final Exception e) {
            LOGGER.error("Could not delete database directory", e);
        }
    }

    private EmbeddedQuestDbStatusHistoryRepository givenTestSubject() {
        final NiFiProperties niFiProperties = Mockito.mock(NiFiProperties.class);

        Mockito.when(niFiProperties.getIntegerProperty(
                NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_NODE_DAYS,
                NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_NODE_DAYS)
        ).thenReturn(DAYS_TO_KEEP_DATA);

        Mockito.when(niFiProperties.getIntegerProperty(
                NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_COMPONENT_DAYS,
                NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_COMPONENT_DAYS)
        ).thenReturn(DAYS_TO_KEEP_DATA);

        Mockito.when(niFiProperties.getQuestDbStatusRepositoryPath()).thenReturn(Paths.get(path));

        final EmbeddedQuestDbStatusHistoryRepository testSubject = new EmbeddedQuestDbStatusHistoryRepository(niFiProperties, PERSIST_FREQUENCY);
        testSubject.start();
        return testSubject;
    }

    protected void givenWaitUntilPersisted() throws InterruptedException {
        Thread.sleep(3000); // The actual writing happens asynchronously on a different thread
    }
}
