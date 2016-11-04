package org.apache.nifi.util.hive;

/**
 * Created by dstreev on 2016-11-03.
 */
public class CsvOutputOptions {

    private boolean header = true;
    private String altHeader = null;
    private String delimiter = ",";
    private boolean quote = false;
    private boolean escape = true;

    public boolean isHeader() {
        return header;
    }

    public String getAltHeader() {
        return altHeader;
    }


    public String getDelimiter() {
        return delimiter;
    }


    public boolean isQuote() {
        return quote;
    }

    public boolean isEscape() {
        return escape;
    }

    public CsvOutputOptions(boolean header, String altHeader, String delimiter, boolean quote, boolean escape) {
        this.header = header;
        this.altHeader = altHeader;
        this.delimiter = delimiter;
        this.quote = quote;
        this.escape = escape;
    }
}
