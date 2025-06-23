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
package org.apache.nifi.record.path.filter;

import org.apache.nifi.record.path.paths.RecordPathSegment;

public abstract class NumericComparisonFilter extends NumericBinaryOperatorFilter {

    public NumericComparisonFilter(final RecordPathSegment lhs, final RecordPathSegment rhs) {
        super(lhs, rhs);
    }

    @Override
    protected final boolean test(final long lhsNumber, final long rhsNumber) {
        final int comparisonResult = Long.compare(lhsNumber, rhsNumber);
        return verifyComparisonResult(comparisonResult);
    }

    @Override
    protected final boolean test(final double lhsNumber, final double rhsNumber) {
        final int comparisonResult = Double.compare(lhsNumber, rhsNumber);
        return verifyComparisonResult(comparisonResult);
    }

    protected abstract boolean verifyComparisonResult(final int comparisonResult);
}
