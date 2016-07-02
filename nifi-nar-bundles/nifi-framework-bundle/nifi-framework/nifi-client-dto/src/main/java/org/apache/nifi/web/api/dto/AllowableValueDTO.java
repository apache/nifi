package org.apache.nifi.web.api.dto;

import com.wordnik.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

/**
 * The allowable values for a property with a constrained set of options.
 */
@XmlType(name = "allowableValue")
public class AllowableValueDTO {

    private String displayName;
    private String value;
    private String description;

    /**
     * @return the human-readable value that is allowed for this PropertyDescriptor
     */
    @ApiModelProperty(
            value = "A human readable value that is allowed for the property descriptor."
    )
    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    /**
     * @return the value for this allowable value
     */
    @ApiModelProperty(
            value = "A value that is allowed for the property descriptor."
    )
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * @return a description of this Allowable Value, or <code>null</code> if no description is given
     */
    @ApiModelProperty(
            value = "A description for this allowable value."
    )
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof AllowableValueDTO)) {
            return false;
        }

        final AllowableValueDTO other = (AllowableValueDTO) obj;
        return (this.value.equals(other.getValue()));
    }

    @Override
    public int hashCode() {
        return 23984731 + 17 * value.hashCode();
    }
}
