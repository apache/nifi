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
package org.apache.nifi.web.api.concurrent;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardAsynchronousWebRequestTest {

    private static final String REQUEST_ID = "request-id";
    private static final String COMPONENT_ID = "component-id";

    @Test
    public void testFailAfterCancelDoesNotOverwriteCancellationReason() {
        final StandardAsynchronousWebRequest<String, String> request = createRequest();

        request.cancel();
        request.fail("Failed while handling interruption from cancellation");

        assertEquals("Request cancelled by user", request.getFailureReason());
        assertTrue(request.isCancelled());
        assertTrue(request.isComplete());
    }

    @Test
    public void testCancelInvokesCancelCallback() {
        final StandardAsynchronousWebRequest<String, String> request = createRequest();
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        request.setCancelCallback(() -> callbackInvoked.set(true));

        request.cancel();

        assertTrue(callbackInvoked.get());
        assertEquals("Request cancelled by user", request.getFailureReason());
    }

    @Test
    public void testFailBeforeCancelSetsFailureReason() {
        final StandardAsynchronousWebRequest<String, String> request = createRequest();

        request.fail("Backlog determination failed");

        assertEquals("Backlog determination failed", request.getFailureReason());
        assertTrue(request.isComplete());
        assertFalse(request.isCancelled());
    }

    private StandardAsynchronousWebRequest<String, String> createRequest() {
        final List<UpdateStep> updateSteps = Collections.singletonList(new StandardUpdateStep("Determining Backlog"));
        return new StandardAsynchronousWebRequest<>(REQUEST_ID, COMPONENT_ID, COMPONENT_ID, null, updateSteps);
    }
}
