package org.apache.nifi.record.path.filter;

import org.apache.nifi.record.path.paths.RecordPathSegment;

public abstract class NumericComparisonFilter extends NumericBinaryOperatorFilter {

    public NumericComparisonFilter(final RecordPathSegment lhs, final RecordPathSegment rhs) {
        super(lhs, rhs);
    }

    @Override
    protected final boolean test(final Number lhsNumber, final Number rhsNumber) {
        final int comparisonResult = isLongCompatible(lhsNumber) && isLongCompatible(rhsNumber)
                ? Long.compare(lhsNumber.longValue(), rhsNumber.longValue())
                : Double.compare(lhsNumber.doubleValue(), rhsNumber.doubleValue());

        return verifyComparisonResult(comparisonResult);
    }

    protected abstract boolean verifyComparisonResult(final int comparisonResult);

    private boolean isLongCompatible(final Number value) {
        return value instanceof Long
                || value instanceof Integer
                || value instanceof Short
                || value instanceof Byte;
    }
}
