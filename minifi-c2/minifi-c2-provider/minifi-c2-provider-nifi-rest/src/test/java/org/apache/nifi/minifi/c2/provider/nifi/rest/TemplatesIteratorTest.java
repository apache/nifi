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

package org.apache.nifi.minifi.c2.provider.nifi.rest;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.common.collect.Lists;
import org.apache.nifi.minifi.c2.api.ConfigurationProviderException;
import org.apache.nifi.minifi.c2.api.util.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TemplatesIteratorTest {
    private JsonFactory jsonFactory;
    private HttpURLConnection httpURLConnection;
    private NiFiRestConnector niFiRestConnector;

    @Before
    public void setup() throws ConfigurationProviderException {
        jsonFactory = new JsonFactory();
        httpURLConnection = mock(HttpURLConnection.class);
        niFiRestConnector = mock(NiFiRestConnector.class);
        when(niFiRestConnector.get(TemplatesIterator.FLOW_TEMPLATES)).thenReturn(httpURLConnection);
    }

    @Test(expected = NoSuchElementException.class)
    public void testIteratorNoSuchElementException() throws ConfigurationProviderException, IOException {
        when(httpURLConnection.getInputStream()).thenReturn(TemplatesIteratorTest.class.getClassLoader().getResourceAsStream("noTemplates.json"));

        try (TemplatesIterator templatesIterator = new TemplatesIterator(niFiRestConnector, jsonFactory)) {
            assertFalse(templatesIterator.hasNext());
            templatesIterator.next();
        } finally {
            verify(httpURLConnection).disconnect();
        }
    }

    @Test
    public void testIteratorNoTemplates() throws ConfigurationProviderException, IOException {
        when(httpURLConnection.getInputStream()).thenReturn(TemplatesIteratorTest.class.getClassLoader().getResourceAsStream("noTemplates.json"));
        List<Pair<String, String>> idToNameList;
        try (TemplatesIterator templatesIterator = new TemplatesIterator(niFiRestConnector, jsonFactory)) {
            idToNameList = Lists.newArrayList(templatesIterator);
        }
        assertEquals(0, idToNameList.size());

        verify(httpURLConnection).disconnect();
    }

    @Test
    public void testIteratorSingleTemplate() throws ConfigurationProviderException, IOException {
        when(httpURLConnection.getInputStream()).thenReturn(TemplatesIteratorTest.class.getClassLoader().getResourceAsStream("oneTemplate.json"));
        List<Pair<String, String>> idToNameList;
        try (TemplatesIterator templatesIterator = new TemplatesIterator(niFiRestConnector, jsonFactory)) {
            idToNameList = Lists.newArrayList(templatesIterator);
        }
        assertEquals(1, idToNameList.size());
        Pair<String, String> idNamePair = idToNameList.get(0);
        assertEquals("d05845ae-ceda-4c50-b7c2-037e42ddf1d3", idNamePair.getFirst());
        assertEquals("raspi3.v1", idNamePair.getSecond());

        verify(httpURLConnection).disconnect();
    }

    @Test
    public void testIteratorTwoTemplates() throws ConfigurationProviderException, IOException {
        when(httpURLConnection.getInputStream()).thenReturn(TemplatesIteratorTest.class.getClassLoader().getResourceAsStream("twoTemplates.json"));
        List<Pair<String, String>> idToNameList;
        try (TemplatesIterator templatesIterator = new TemplatesIterator(niFiRestConnector, jsonFactory)) {
            idToNameList = Lists.newArrayList(templatesIterator);
        }
        assertEquals(2, idToNameList.size());
        Pair<String, String> idNamePair = idToNameList.get(0);
        assertEquals("d05845ae-ceda-4c50-b7c2-037e42ddf1d3", idNamePair.getFirst());
        assertEquals("raspi3.v1", idNamePair.getSecond());

        idNamePair = idToNameList.get(1);
        assertEquals("9384b48d-85b4-478a-bf3e-64d113f8fbc5", idNamePair.getFirst());
        assertEquals("raspi3.v2", idNamePair.getSecond());

        verify(httpURLConnection).disconnect();
    }
}
