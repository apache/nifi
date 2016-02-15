package org.apache.nifi.processors.aws.dynamodb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Utility class to keep a map of keys and flow files
 */
public class ItemKeys {

    protected Object hashKey = "";
    protected Object rangeKey = "";

    public ItemKeys(Object hashKey, Object rangeKey) {
        if ( hashKey != null )
            this.hashKey = hashKey;
        if ( rangeKey != null )
            this.rangeKey = rangeKey;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, false);
    }

    @Override
    public boolean equals(Object other) {
        return EqualsBuilder.reflectionEquals(this, other, false);
    }
}
