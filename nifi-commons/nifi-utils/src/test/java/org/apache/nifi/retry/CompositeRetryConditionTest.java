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
package org.apache.nifi.retry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collections;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class CompositeRetryConditionTest {

    @Mock
    public RetryExecutionContext context;

    @Mock
    public RetryCondition condition1;

    @Mock
    public RetryCondition condition2;

    @Test
    public void testAllConditionsAllow() {
        final CompositeRetryCondition testSubject = new CompositeRetryCondition(Arrays.asList(condition1, condition2));
        Mockito.when(condition1.allowNextAttempt(Mockito.any(RetryExecutionContext.class))).thenReturn(true);
        Mockito.when(condition2.allowNextAttempt(Mockito.any(RetryExecutionContext.class))).thenReturn(true);

        Assertions.assertTrue(testSubject.allowNextAttempt(context));
    }

    @Test
    public void testSomeConditionDisallows() {
        final CompositeRetryCondition testSubject = new CompositeRetryCondition(Arrays.asList(condition1, condition2));
        Mockito.when(condition1.allowNextAttempt(Mockito.any(RetryExecutionContext.class))).thenReturn(true);
        Mockito.when(condition2.allowNextAttempt(Mockito.any(RetryExecutionContext.class))).thenReturn(false);

        Assertions.assertFalse(testSubject.allowNextAttempt(context));
    }

    @Test
    public void testEmptyConditionListAllows() {
        final CompositeRetryCondition testSubject = new CompositeRetryCondition(Collections.emptyList());
        Assertions.assertTrue(testSubject.allowNextAttempt(context));
    }
}