package org.apache.nifi.processors.standard.db;

import org.apache.nifi.components.DescribedValue;

/**
 * Enumeration of supported Database column name Translation Strategy
 */
public enum TranslationStrategy implements DescribedValue {
    REMOVE_UNDERSCORE("REMOVE_UNDERSCORE","Remove Underscore","Underscore(_) will be removed from column name with empty string Ex. Pics_1_11 become PICS111"),
    REMOVE_SPACE("REMOVE_SPACE","Remove Space","Spaces will be removed from column name with empty string Ex. 'User Name' become 'USERNAME'"),
    REMOVE_ALL_SPECIAL_CHAR("REMOVE_ALL_SPECIAL_CHAR","Remove All Special Character","Remove All Special Character"),
    REGEX("REGEX","Regular Expression","Remove character matched Regular Expression from column name");
    private final String value;

    private final String displayName;

    private final String description;
    TranslationStrategy(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}