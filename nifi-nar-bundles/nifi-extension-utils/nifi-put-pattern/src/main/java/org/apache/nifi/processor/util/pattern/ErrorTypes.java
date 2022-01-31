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

import static org.apache.nifi.processor.util.pattern.ErrorTypes.Destination.Failure;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Destination.ProcessException;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Destination.Retry;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Destination.Self;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Penalty.None;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Penalty.Penalize;
import static org.apache.nifi.processor.util.pattern.ErrorTypes.Penalty.Yield;

/**
 * Represents general error types and how it should be treated.
 */
public enum ErrorTypes {

    /**
     * Procedure setting has to be fixed, otherwise the same error would occur irrelevant to the input.
     * In order to NOT call failing process frequently, this should be yielded.
     */
    PersistentFailure(ProcessException, Yield),

    /**
     * It is unknown whether the error is persistent or temporal, related to the input or not.
     */
    UnknownFailure(ProcessException, None),

    /**
     * The input will be sent to the failure route for recovery without penalizing.
     * Basically, the input should not be sent to the same procedure again unless the issue has been solved.
     */
    InvalidInput(Failure, None),

    /**
     * The procedure is temporarily unavailable, usually due to the external service unavailability.
     * Retrying maybe successful, but it should be yielded for a while.
     */
    TemporalFailure(Retry, Yield),

    /**
     * The input was not processed successfully due to some temporal error
     * related to the specifics of the input. Retrying maybe successful,
     * but it should be penalized for a while.
     */
    TemporalInputFailure(Retry, Penalize),

    /**
     * The input was not ready for being processed. It will be kept in the incoming queue and also be penalized.
     */
    Defer(Self, Penalize);

    private final Destination destination;
    private final Penalty penalty;
    ErrorTypes(Destination destination, Penalty penalty){
        this.destination = destination;
        this.penalty = penalty;
    }

    public Result result() {
        return new Result(destination, penalty);
    }

    /**
     * Represents the destination of input.
     */
    public enum Destination {
        ProcessException, Failure, Retry, Self
    }

    /**
     * Indicating yield or penalize the processing when transfer the input.
     */
    public enum Penalty {
        Yield, Penalize, None
    }

    public Destination destination(){
        return this.destination;
    }

    public Penalty penalty(){
        return this.penalty;
    }

    /**
     * Result represents a result of a procedure.
     * ErrorTypes enum contains basic error result patterns.
     */
    public static class Result {
        private final Destination destination;
        private final Penalty penalty;

        public Result(Destination destination, Penalty penalty) {
            this.destination = destination;
            this.penalty = penalty;
        }

        public Destination destination() {
            return destination;
        }

        public Penalty penalty() {
            return penalty;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "destination=" + destination +
                    ", penalty=" + penalty +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Result result = (Result) o;

            if (destination != result.destination) return false;
            return penalty == result.penalty;
        }

        @Override
        public int hashCode() {
            int result = destination != null ? destination.hashCode() : 0;
            result = 31 * result + (penalty != null ? penalty.hashCode() : 0);
            return result;
        }
    }

}
