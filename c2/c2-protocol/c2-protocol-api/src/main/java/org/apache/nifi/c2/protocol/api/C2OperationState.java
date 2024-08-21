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

package org.apache.nifi.c2.protocol.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.Objects;

/**
 * Simple model of operations. The current approach is to capture a shared state ( via agent(s)
 * and C2 server. This operation ID correspond with an operation requested of an agent. The OperationState
 * is the state of the operation at response time. The phrase 'state' is intentional to imply that
 * the state of the operation is transient and can potentially be different across attempts.
 * <p>
 * This may suggest that a retry interval/attempt be added. Additionally, the details may provide user
 * feedback; however, it may be useful to separate failures into pre and post conditions. Details may provide
 * some insight, but a pre-condition and post-condition failure may better indicate how to arrive at operational
 * success.
 */
public class C2OperationState implements Serializable {
    private static final long serialVersionUID = 1L;

    @Schema(description = "State of the operation performed", example = "FULLY_APPLIED")
    private OperationState state;

    @Schema(description = "Additional details about the state")
    private String details;

    @Schema(description = "Additional details about the cause of the failure")
    private FailureCause failureCause;

    public FailureCause getFailureCause() {
        return failureCause;
    }

    public void setFailureCause(FailureCause failureCause) {
        this.failureCause = failureCause;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(final String details) {
        this.details = details;
    }

    public OperationState getState() {
        return state;
    }

    public void setState(final OperationState state) {
        this.state = state;
    }

    /**
     * Sets the operation state via the ordinal value.
     *
     * @param state ordinal value of this operation state.
     */
    public void setStateFromOrdinal(int state) {
        this.state = OperationState.fromOrdinal(state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        C2OperationState that = (C2OperationState) o;
        return state == that.state && Objects.equals(details, that.details) && Objects.equals(failureCause, that.failureCause);
    }

    @Override
    public int hashCode() {
        return Objects.hash(state, details, failureCause);
    }

    @Override
    public String toString() {
        return "C2OperationState{" +
            "state=" + state +
            ", details='" + details + '\'' +
            ", failureCause='" + failureCause + '\'' +
            '}';
    }

    public enum OperationState {
        FULLY_APPLIED,
        PARTIALLY_APPLIED,
        OPERATION_NOT_UNDERSTOOD,
        NO_OPERATION,
        NOT_APPLIED;

        /**
         * We cannot rely on ordering within the ordinal set to translate
         * we must use a translation method as we're interconnecting between
         * different languages.
         *
         * @param state input ordinal
         * @return update state enumeration value.
         */
        public static OperationState fromOrdinal(int state) {
            switch (state) {
                case 0:
                    return FULLY_APPLIED;
                case 1:
                    return PARTIALLY_APPLIED;
                case 2:
                    return OPERATION_NOT_UNDERSTOOD;
                case 3:
                    return NO_OPERATION;
                case 4:
                default:
                    return NOT_APPLIED;
            }
        }

        /**
         * We cannot rely on ordering within the ordinal set to translate
         * we must use a translation method as we're interconnecting between
         * different languages ( for binary protocols ).
         *
         * @param state enumeration value
         * @return predefined ordinal
         */
        public static int toOrdinal(OperationState state) {
            switch (state) {
                case FULLY_APPLIED:
                    return 0;
                case PARTIALLY_APPLIED:
                    return 1;
                case OPERATION_NOT_UNDERSTOOD:
                    return 2;
                case NO_OPERATION:
                    return 3;
                case NOT_APPLIED:
                default:
                    return 4;
            }
        }
    }

}
