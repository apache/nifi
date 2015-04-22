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
package org.apache.nifi.controller.status.history;

import java.util.List;

public class StandardMetricDescriptor<T> implements MetricDescriptor<T> {

    private final String field;
    private final String label;
    private final String description;
    private final MetricDescriptor.Formatter formatter;
    private final ValueMapper<T> valueMapper;
    private final ValueReducer<StatusSnapshot, Long> reducer;

    public StandardMetricDescriptor(final String field, final String label, final String description, final MetricDescriptor.Formatter formatter, final ValueMapper<T> valueFunction) {
        this(field, label, description, formatter, valueFunction, null);
    }

    public StandardMetricDescriptor(final String field, final String label, final String description,
            final MetricDescriptor.Formatter formatter, final ValueMapper<T> valueFunction, final ValueReducer<StatusSnapshot, Long> reducer) {
        this.field = field;
        this.label = label;
        this.description = description;
        this.formatter = formatter;
        this.valueMapper = valueFunction;
        this.reducer = reducer == null ? new SumReducer() : reducer;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public MetricDescriptor.Formatter getFormatter() {
        return formatter;
    }

    @Override
    public ValueMapper<T> getValueFunction() {
        return valueMapper;
    }

    @Override
    public ValueReducer<StatusSnapshot, Long> getValueReducer() {
        return reducer;
    }

    @Override
    public String toString() {
        return "StandardMetricDescriptor[" + label + "]";
    }

    @Override
    public int hashCode() {
        return 23987 + formatter.name().hashCode() + 4 * label.hashCode() + 8 * field.hashCode() + 28 * description.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MetricDescriptor)) {
            return false;
        }

        MetricDescriptor<?> other = (MetricDescriptor<?>) obj;
        return field.equals(other.getField());
    }

    class SumReducer implements ValueReducer<StatusSnapshot, Long> {

        @Override
        public Long reduce(final List<StatusSnapshot> values) {
            long sum = 0;
            for (final StatusSnapshot snapshot : values) {
                sum += snapshot.getStatusMetrics().get(StandardMetricDescriptor.this);
            }

            return sum;
        }
    }
}
