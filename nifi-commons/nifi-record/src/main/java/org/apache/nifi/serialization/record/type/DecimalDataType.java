package org.apache.nifi.serialization.record.type;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class DecimalDataType extends DataType {

    private final int precision;
    private final int scale;

    public DecimalDataType(int precision, int scale) {
        super(RecordFieldType.DECIMAL, null);
        this.precision = precision;
        this.scale = scale;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.DECIMAL;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getFieldType().hashCode() + 41 * Integer.hashCode(precision) * Integer.hashCode(scale); // Need to confirm this hash * hash is correct
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DecimalDataType)) {
            return false;
        }

        final DecimalDataType other = (DecimalDataType) obj;
        return precision == other.getPrecision() && scale == other.getScale();
    }

    @Override
    public String toString() {
        return "DECIMAL" + Integer.toString(precision) + Integer.toString(scale);
    }
}
