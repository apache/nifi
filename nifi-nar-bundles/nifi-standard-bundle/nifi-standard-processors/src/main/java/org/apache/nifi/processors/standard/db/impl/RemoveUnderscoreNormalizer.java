package org.apache.nifi.processors.standard.db.impl;

import org.apache.nifi.processors.standard.db.ColumnNameNormalizer;

public class RemoveUnderscoreNormalizer implements ColumnNameNormalizer {


    @Override
    public String getNormalizedName(String colName) {
        return  colName.toUpperCase().replace("_", "");
    }
}
