package org.apache.nifi.processors.standard.db.impl;

import org.apache.nifi.processors.standard.db.ColumnNameNormalizer;

import java.util.regex.Pattern;

public class PatternNormalizer implements ColumnNameNormalizer {
    private Pattern pattern;

    public PatternNormalizer(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public String getNormalizedName(String colName) {

        if (pattern == null) {
            return colName;
        } else {
            return pattern.matcher(colName.toUpperCase()).replaceAll("");
        }
    }

}
