package org.apache.nifi.processors.standard.db;

import java.util.Objects;

public class ColumnNameNormalizer {

    private final boolean isTranslationEnabled;
    private final String translationStrategy;
    private final String translationRegex;
    private final String REMOVE_ALL_SPECIAL_CHAR_REGEX = "[^a-zA-Z0-9]";

    public ColumnNameNormalizer(boolean isTranslationEnabled, String translationStrategy, String translationRegex) {
        this.isTranslationEnabled = isTranslationEnabled;
        this.translationStrategy = translationStrategy;
        this.translationRegex = translationRegex;
    }

    public String getColName(final String colName) {
        if (colName == null || !isTranslationEnabled) {
            return colName;
        }

        TranslationStrategy strategy = TranslationStrategy.fromString(translationStrategy);
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

    // Enum representing the possible translation strategies
    private enum TranslationStrategy {
        REMOVE_UNDERSCORE,
        REMOVE_SPACE,
        REMOVE_ALL_SPECIAL_CHAR,
        REGEX;

        // Helper method to convert the given string to the corresponding enum value
        public static TranslationStrategy fromString(String value) {
            try {
                return TranslationStrategy.valueOf(value.toUpperCase());
            } catch (IllegalArgumentException | NullPointerException e) {
                return null;
            }
        }
    }
}