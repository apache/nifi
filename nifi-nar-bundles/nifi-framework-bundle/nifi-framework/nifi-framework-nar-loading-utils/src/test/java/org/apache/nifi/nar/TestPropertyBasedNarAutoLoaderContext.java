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
package org.apache.nifi.nar;

import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;

public class TestPropertyBasedNarAutoLoaderContext {
    private static final String VALID_PARAMETER_NAME = "nifi.library.nar.autoload.test";
    private static final String VALID_PARAMETER_VALUE = "loremIpsum";

    @Test
    public void testGetAutoLoadDirectory() {
        // given
        final File autoLoadDirectory = new File(".");
        final NiFiProperties properties = Mockito.mock(NiFiProperties.class);
        final PropertyBasedNarAutoLoaderContext testSubject = new PropertyBasedNarAutoLoaderContext(properties);
        Mockito.when(properties.getNarAutoLoadDirectory()).thenReturn(autoLoadDirectory);

        // when
        final File result = testSubject.getAutoLoadDirectory();

        // then
        Mockito.verify(properties, Mockito.times(1)).getNarAutoLoadDirectory();
        Assert.assertEquals(autoLoadDirectory, result);
    }

    @Test
    public void testAllowedParameter() {
        // given
        final NiFiProperties properties = Mockito.mock(NiFiProperties.class);
        final PropertyBasedNarAutoLoaderContext testSubject = new PropertyBasedNarAutoLoaderContext(properties);
        Mockito.when(properties.getProperty(VALID_PARAMETER_NAME)).thenReturn(VALID_PARAMETER_VALUE);

        // when
        final String result = testSubject.getParameter(VALID_PARAMETER_NAME);

        // then
        Mockito.verify(properties, Mockito.times(1)).getProperty(VALID_PARAMETER_NAME);
        Assert.assertEquals(VALID_PARAMETER_VALUE, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNotAllowedParameter() {
        // given
        final NiFiProperties properties = Mockito.mock(NiFiProperties.class);
        final PropertyBasedNarAutoLoaderContext testSubject = new PropertyBasedNarAutoLoaderContext(properties);

        // when
        testSubject.getParameter("invalid");
    }
}