package org.apache.nifi.processors.standard.db.impl;

import org.apache.nifi.processors.standard.db.ColumnNameNormalizer;

import java.util.regex.Pattern;

public class RemoveAllSpecialCharNormalizer implements ColumnNameNormalizer {
    private static final Pattern REMOVE_ALL_SPECIAL_CHAR_REGEX = Pattern.compile("[^a-zA-Z0-9]");
    @Override
    public String getNormalizedName(String colName) {
        return REMOVE_ALL_SPECIAL_CHAR_REGEX.matcher(colName.toUpperCase()).replaceAll("");
    }
}
