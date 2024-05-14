package org.apache.nifi.py4j.logback; /*
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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.nifi.py4j.logging.LogLevelChangeListener;
import org.apache.nifi.py4j.logging.StandardLogLevelChangeHandler;
import org.apache.nifi.logging.LogLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LevelChangeListenerTest {

    private static final String LOGGER_NAME = Test.class.getName();

    @Mock
    private Logger logger;

    @Mock
    private LogLevelChangeListener logLevelChangeListener;

    private LevelChangeListener listener;

    @BeforeEach
    void setListener() {
        listener = new LevelChangeListener(StandardLogLevelChangeHandler.getHandler());

        StandardLogLevelChangeHandler.getHandler().addListener(LogLevelChangeListener.class.getSimpleName(), logLevelChangeListener);
    }

    @AfterEach
    void removeListener() {
        StandardLogLevelChangeHandler.getHandler().removeListener(LogLevelChangeListener.class.getSimpleName());
    }

    @Test
    void testOnLevelChange() {
        when(logger.getName()).thenReturn(LOGGER_NAME);

        listener.onLevelChange(logger, Level.INFO);

        verify(logLevelChangeListener).onLevelChange(eq(LOGGER_NAME), eq(LogLevel.INFO));
    }
}
