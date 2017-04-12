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
package org.apache.nifi.processor.util.pattern;

import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(TestExceptionHandler.class);

    /**
     * Simulate an external procedure.
     */
    static class ExternalProcedure {
        private boolean available = true;
        int divide(Integer a, Integer b) throws Exception {
            if (!available) {
                throw new IOException("Not available");
            }
            if (a == 10) {
                throw new IllegalStateException("Service for 10 is not currently available.");
            }
            return a / b;
        }
    }

    private class Context {
        int count = 0;
    }

    @Test
    public void testBasicUsage() {

        final ExternalProcedure p = new ExternalProcedure();

        try {
            // Although a catch-exception has to be caught each possible call,
            // usually the error handling logic will be the same.
            // Ends up having a lot of same code.
            final int r1 = p.divide(4, 2);
            assertEquals(2, r1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Context context = new Context();
        final ExceptionHandler<Context> handler = new ExceptionHandler<>();

        // Using handler can avoid the try catch block with reusable error handling logic.
        handler.execute(context, 6, i -> {
            final int r2 = p.divide(i, 2);
            assertEquals(3, r2);
        });

        // If return value is needed, use AtomicReference.
        AtomicReference<Integer> r = new AtomicReference<>();
        handler.execute(context, 8, i -> r.set(p.divide(i, 2)));
        assertEquals(4, r.get().intValue());

        // If no exception mapping is specified, any Exception thrown is wrapped by ProcessException.
        try {
            final Integer nullInput = null;
            handler.execute(context, nullInput, i -> r.set(p.divide(i, 2)));
            fail("Exception should be thrown because input is null.");
        } catch (ProcessException e) {
            assertTrue(e.getCause() instanceof NullPointerException);
        }
    }

    // Reusable Exception mapping function.
    static Function<Exception, ErrorTypes> exceptionMapping = i -> {
        try {
            throw i;
        } catch (NullPointerException | ArithmeticException | NumberFormatException e) {
            return ErrorTypes.InvalidInput;
        } catch (IllegalStateException e) {
            return ErrorTypes.TemporalInputFailure;
        } catch (IOException e) {
            return ErrorTypes.TemporalFailure;
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    };

    @Test
    public void testHandling() {

        final ExternalProcedure p = new ExternalProcedure();
        final Context context = new Context();

        final ExceptionHandler<Context> handler = new ExceptionHandler<>();
        handler.mapException(exceptionMapping);
        handler.onError(createInputErrorHandler());

        // Benefit of handler is being able to externalize error handling, make it simpler.
        handler.execute(context, 4, i -> {
            final int r = p.divide(i, 2);
            assertEquals(2, r);
        });

        // Null pointer exception.
        final Integer input = null;
        handler.execute(context, input, i -> {
            p.divide(i, 2);
            fail("Shouldn't reach here.");
        });

        // Divide by zero.
        handler.execute(context, 0, i -> {
            p.divide(2, i);
            fail("Shouldn't reach here.");
        });


    }

    static <C> ExceptionHandler.OnError<C, Integer> createInputErrorHandler() {
        return (c, i, r, e) -> {
            switch (r.destination()) {
                case ProcessException:
                    throw new ProcessException(String.format("Execution failed due to %s", e), e);
                default:
                    logger.warn(String.format("Routing to %s: %d caused %s", r, i, e));
            }
        };
    }

    static <C> ExceptionHandler.OnError<C, Integer[]> createArrayInputErrorHandler() {
        return (c, i, r, e) -> {
            switch (r.destination()) {
                case ProcessException:
                    throw new ProcessException(String.format("Execution failed due to %s", e), e);
                default:
                    logger.warn(String.format("Routing to %s: %d, %d caused %s", r, i[0], i[1], e));
            }
        };
    }

    @Test
    public void testHandlingLoop() {

        final ExternalProcedure p = new ExternalProcedure();
        final Context context = new Context();

        final ExceptionHandler<Context> handler = new ExceptionHandler<>();
        handler.mapException(exceptionMapping);
        handler.onError(createArrayInputErrorHandler());

        // It's especially handy when looping through inputs. [a, b, expected result]
        Integer[][] inputs = new Integer[][]{{4, 2, 2}, {null, 2, 999}, {2, 0, 999}, {10, 2, 999}, {8, 2, 4}};

        Arrays.stream(inputs).forEach(input ->  handler.execute(context, input, (in) -> {
            final Integer r = p.divide(in[0], in[1]);
            // This is safe because if p.divide throws error, this code won't be executed.
            assertEquals(in[2], r);
        }));

        AtomicReference<Integer> r = new AtomicReference<>();
        for (Integer[] input : inputs) {

            if (!handler.execute(context, input, (in) -> {
                r.set(p.divide(in[0], in[1]));
                context.count++;
            })){
                // Handler returns false when it fails.
                // Cleaner if-exception-continue-next-input can be written cleaner.
                continue;
            }

            assertEquals(input[2], r.get());
        }

        assertEquals("Successful inputs", 2, context.count);
    }

}
