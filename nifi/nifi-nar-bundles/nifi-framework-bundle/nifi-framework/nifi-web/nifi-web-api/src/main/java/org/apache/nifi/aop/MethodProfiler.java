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
package org.apache.nifi.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MethodProfiler {

    private static final Logger logger = LoggerFactory.getLogger(MethodProfiler.class);

    public Object profileMethod(final ProceedingJoinPoint call) throws Throwable {
        final long startTime = System.nanoTime();
        try {
            return call.proceed();
        } finally {
            if (logger.isDebugEnabled()) {
                final long endTime = System.nanoTime();
                final long durationMilliseconds = (endTime - startTime) / 1000000;
                final String methodCall = call.getSignature().toLongString();
                logger.debug("'" + methodCall + "' Time to complete call (ms): " + durationMilliseconds);
            }
        }
    }

}
