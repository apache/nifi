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
package org.apache.nifi.bootstrap.process;

public class RuntimeValidatorResult  {
    private final String subject;
    private final String explanation;
    private final Outcome outcome;
    protected RuntimeValidatorResult(final Builder builder) {
        this.subject = builder.subject;
        this.explanation = builder.explanation;
        this.outcome = builder.outcome;
    }

    public String getSubject() {
        return subject;
    }

    public String getExplanation() {
        return explanation;
    }

    public Outcome getOutcome() {
        return outcome;
    }

    public static final class Builder {
        private String subject = "";
        private String explanation = "";
        private Outcome outcome = Outcome.FAILED;

        public Builder subject(final String subject) {
            if (subject != null) {
                this.subject = subject;
            }
            return this;
        }

        public Builder explanation(final String explanation) {
            if (explanation != null) {
                this.explanation = explanation;
            }
            return this;
        }

        public Builder outcome(final Outcome outcome) {
            this.outcome = outcome;
            return this;
        }

        public RuntimeValidatorResult build() {
            return new RuntimeValidatorResult(this);
        }
    }

    public enum Outcome {
        SUCCESSFUL,

        FAILED,

        SKIPPED;
    }
}
