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

public class StandardMetricDescriptor<T> extends AbstractMetricDescriptor<T> {

    public StandardMetricDescriptor(final IndexableMetric indexableMetric, final String field, final String label, final String description,
                                    final MetricDescriptor.Formatter formatter, final ValueMapper<T> valueFunction) {
        super(indexableMetric, field, label, description, formatter, valueFunction);
    }

    public StandardMetricDescriptor(final IndexableMetric indexableMetric, final String field, final String label, final String description,
                                    final MetricDescriptor.Formatter formatter, final ValueMapper<T> valueFunction, final ValueReducer<StatusSnapshot, Long> reducer) {
        super(indexableMetric, field, label, description, formatter, valueFunction, reducer);
    }

    @Override
    public boolean isCounter() {
        return false;
    }

    @Override
    public String toString() {
        return "StandardMetricDescriptor[" + getLabel() + "]";
    }

    @Override
    public int hashCode() {
        return 23987 + getFormatter().name().hashCode() + 4 * getLabel().hashCode() + 8 * getField().hashCode() + 28 * getDescription().hashCode();
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
        return getField().equals(other.getField());
    }

}
