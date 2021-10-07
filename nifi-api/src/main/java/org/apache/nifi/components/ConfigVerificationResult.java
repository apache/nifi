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

package org.apache.nifi.components;

public class ConfigVerificationResult {
    private final Outcome outcome;
    private final String verificationStepName;
    private final String explanation;

    private ConfigVerificationResult(final Builder builder) {
        outcome = builder.outcome;
        verificationStepName = builder.verificationStepName;
        explanation = builder.explanation;
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public String getVerificationStepName() {
        return verificationStepName;
    }

    public String getExplanation() {
        return explanation;
    }

    @Override
    public String toString() {
        return "ConfigVerificationResult[" +
            "outcome=" + outcome +
            ", verificationStepName='" + verificationStepName + "'" +
            ", explanation='" + explanation + "']";
    }

    public static class Builder {
        private Outcome outcome = Outcome.SKIPPED;
        private String verificationStepName = "Unknown Step Name";
        private String explanation;

        public Builder outcome(final Outcome outcome) {
            this.outcome = outcome;
            return this;
        }

        public Builder verificationStepName(final String verificationStepName) {
            this.verificationStepName = verificationStepName;
            return this;
        }

        public Builder explanation(final String explanation) {
            this.explanation = explanation;
            return this;
        }

        public ConfigVerificationResult build() {
            return new ConfigVerificationResult(this);
        }
    }

    public enum Outcome {
        SUCCESSFUL,

        FAILED,

        SKIPPED;
    }
}
