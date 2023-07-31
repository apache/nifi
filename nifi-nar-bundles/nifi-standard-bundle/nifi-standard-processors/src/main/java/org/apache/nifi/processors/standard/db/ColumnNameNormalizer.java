package org.apache.nifi.processors.standard.db;

import java.util.Objects;

/**
 * ColumnNameNormalizer is a utility class that helps to normalize column names. It provides various strategies to modify column names based on the TranslationStrategy enum.
 * Column names can be normalized by removing underscores, spaces, all special characters, or by using a custom regular expression defined by the user.
 */
public class ColumnNameNormalizer {

    // Variables to store the configuration settings
    private final boolean isTranslationEnabled; // Indicates whether the translation is enabled or not.
    private final TranslationStrategy strategy; // The strategy to be used for column name normalization.
    private final String translationRegex; // A custom regular expression for normalization, used when the strategy is set to REGEX.

    // Regular expression to remove all special characters from a string.
    private final String REMOVE_ALL_SPECIAL_CHAR_REGEX = "[^a-zA-Z0-9]";

    public ColumnNameNormalizer(boolean isTranslationEnabled, TranslationStrategy strategy, String translationRegex) {
        this.isTranslationEnabled = isTranslationEnabled;
        this.strategy = strategy;
        this.translationRegex = translationRegex;
    }

    /**
     * Normalizes the given column name based on the specified strategy.
     *
     * @param colName The column name to be normalized.
     * @return The normalized column name as a String.
     */
    public String getColName(final String colName) {
        // If the column name is null or translation is not enabled, return the original column name.
        if (colName == null || !isTranslationEnabled) {
            return colName;
        }
        String result;

        switch (Objects.requireNonNull(strategy)) {
            case REMOVE_UNDERSCORE:
                result = colName.toUpperCase().replace("_", "");
                break;
            case REMOVE_SPACE:
                result = colName.toUpperCase().replace(" ", "");
                break;
            case REMOVE_ALL_SPECIAL_CHAR:
                result = colName.toUpperCase().replaceAll(REMOVE_ALL_SPECIAL_CHAR_REGEX, "");
                break;
            case REGEX:
                result = colName.toUpperCase().replaceAll(translationRegex, "");
                break;
            default:
                result = colName;
                break;
        }

        return result;
    }

}