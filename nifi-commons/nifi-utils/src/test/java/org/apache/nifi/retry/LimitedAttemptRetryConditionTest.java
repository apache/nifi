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

@ExtendWith(MockitoExtension.class)
class LimitedAttemptRetryConditionTest {

    @Mock
    public RetryExecutionContext context;

    @Test
    public void testCreationWithZeroAttempt() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new LimitedAttemptRetryCondition(0));
    }

    @Test
    public void testCreationWithNegativeAttempt() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> new LimitedAttemptRetryCondition(-1));
    }

    @Test
    public void testAttempts() {
        final LimitedAttemptRetryCondition testSubject = new LimitedAttemptRetryCondition(2);
        Mockito.when(context.getNumberOfAttempts()).thenReturn(0, 1, 2, 3);
        Assertions.assertTrue(testSubject.allowNextAttempt(context));
        Assertions.assertTrue(testSubject.allowNextAttempt(context));
        Assertions.assertTrue(testSubject.allowNextAttempt(context));
        Assertions.assertFalse(testSubject.allowNextAttempt(context));
    }
}