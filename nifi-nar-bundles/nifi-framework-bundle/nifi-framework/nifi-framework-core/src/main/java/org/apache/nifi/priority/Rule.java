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
package org.apache.nifi.priority;

import org.apache.nifi.attribute.expression.language.PreparedQuery;
import org.apache.nifi.attribute.expression.language.Query;
import org.apache.nifi.attribute.expression.language.exception.AttributeExpressionLanguageParsingException;

import java.util.Objects;
import java.util.UUID;

public class Rule implements Comparable<Rule> {
    public final static int DEFAULT_RATE_OF_THREAD_USAGE = 20;

    // UNEVALUATED is meant to be used when a FlowFileRecord has yet to be evaluated against any rules. Typically when it
    // first enters the system. Do not stagger these files.
    public static final Rule UNEVALUATED = new Rule("unevaluated", 100) {
        @Override
        public Rule setExpired(boolean expired) {
            return this;
        }
    };

    // DEFAULT is meant to be used when a FlowFileRecord has been evaluated against all rules but did not match any.
    public static final Rule DEFAULT = new Rule("default", DEFAULT_RATE_OF_THREAD_USAGE) {
        @Override
        public Rule setExpired(boolean expired) {
            return this;
        }
    };
    private final String uuid;
    private final String expression;
    private final PreparedQuery query;
    private volatile String label;
    // How frequently to allow a thread to process data associated with this rule.
    // A rateOfThreadUsage of 100 or greater is the equivalent to always process.
    private volatile int rateOfThreadUsage;
    private volatile boolean expired = false;

    public Rule(String label, int rateOfThreadUsage) {
        expression = null;
        query = null;
        this.label = label;
        this.rateOfThreadUsage = rateOfThreadUsage;
        this.uuid = UUID.randomUUID().toString();
    }

    public Rule(String expression, String label, int rateOfThreadUsage, String uuid, boolean expired) {
        this.expression = expression;
        this.label = label;
        this.rateOfThreadUsage = rateOfThreadUsage;
        this.uuid = uuid;
        this.query = Query.prepare(expression);
        this.expired = expired;
    }

    public String getUuid() {
        return uuid;
    }

    public String getExpression() {
        return expression;
    }

    public PreparedQuery getQuery() {
        return query;
    }

    public String getLabel() {
        return label;
    }

    public Rule setLabel(String label) {
        this.label = label;
        return this;
    }

    public int getRateOfThreadUsage() {
        return rateOfThreadUsage;
    }

    public boolean isExpired() {
        return expired;
    }

    public Rule setExpired(boolean expired) {
        this.expired = expired;
        return this;
    }

    public Rule setRateOfThreadUsage(int rateOfThreadUsage) {
        if(rateOfThreadUsage > 0) {
            this.rateOfThreadUsage = rateOfThreadUsage;
        }

        return this;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "uuid='" + uuid + '\'' +
                ", expression='" + expression + '\'' +
                ", query=" + query +
                ", label='" + label + '\'' +
                ", rateOfThreadUsage=" + rateOfThreadUsage +
                ", expired=" + expired +
                '}';
    }

    public boolean equivalent(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        Rule rule = (Rule)o;
        return rateOfThreadUsage == rule.getRateOfThreadUsage() && expression.equals(rule.getExpression());
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) return true;
        if(o == null || getClass() != o.getClass()) return false;
        Rule rule = (Rule)o;
        return Objects.equals(uuid, rule.getUuid());
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public int compareTo(Rule rule) {
        if(rule == null) {
            return 1;
        }

        return Integer.compare(rule.getRateOfThreadUsage(), rateOfThreadUsage);
    }

    public static class Builder {
        String expression = "";
        String label;
        int rateOfThreadUsage = Rule.DEFAULT_RATE_OF_THREAD_USAGE;
        String uuid;
        boolean expired = false;

        public Builder() { }

        public Builder(String expression) {
            this.expression = expression;
        }

        public Builder(Rule originalRule) {
            this.expression = originalRule.getExpression();
            this.label = originalRule.getLabel();
            this.rateOfThreadUsage = originalRule.getRateOfThreadUsage();
        }

        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        public Builder label(String label) {
            this.label = label;
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder rateOfThreadUsage(int rateOfThreadUsage) {
            this.rateOfThreadUsage = rateOfThreadUsage;
            return this;
        }

        public Builder expired(boolean expired) {
            this.expired = expired;
            return this;
        }

        public Rule build() throws IllegalArgumentException {
            try {
                Query.validateExpression(expression, false);
                if(rateOfThreadUsage <= 0) {
                    throw new IllegalArgumentException("Priority value must be greater than 0");
                }
            } catch (AttributeExpressionLanguageParsingException e) {
                throw new IllegalArgumentException(e);
            }
            label = label == null ? "" : label;
            if(uuid == null) {
                uuid = UUID.randomUUID().toString();
            }
            return new Rule(expression, label, rateOfThreadUsage, uuid, expired);
        }
    }
}
