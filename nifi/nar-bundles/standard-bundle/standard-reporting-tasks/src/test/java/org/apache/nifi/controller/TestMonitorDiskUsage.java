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
package org.apache.nifi.controller;

import org.apache.nifi.controller.MonitorDiskUsage;
import static org.junit.Assert.assertEquals;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.reporting.ReportingContext;
import org.apache.nifi.reporting.Severity;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestMonitorDiskUsage {

    @Test
    public void testGeneratesMessageIfTooFull() {
        final AtomicInteger callCounter = new AtomicInteger(0);

        final ReportingContext context = Mockito.mock(ReportingContext.class);
        Mockito.doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final String message = (String) invocation.getArguments()[2];
                System.out.println(message);
                callCounter.incrementAndGet();
                return null;
            }

        }).when(context).createBulletin(Mockito.any(String.class), Mockito.any(Severity.class), Mockito.any(String.class));

        final BulletinRepository brepo = Mockito.mock(BulletinRepository.class);
        Mockito.doNothing().when(brepo).addBulletin(Mockito.any(Bulletin.class));
        Mockito.doReturn(brepo).when(context).getBulletinRepository();

        MonitorDiskUsage.checkThreshold("Test Path", Paths.get("."), 0, context);
        assertEquals(1, callCounter.get());
    }

}
