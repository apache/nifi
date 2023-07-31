package org.apache.nifi.processors.standard.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class ColumnNameNormalizerTest {

    private ColumnNameNormalizer normalizer;

    @Test
    void testNormalizingColumnName_RemoveUnderscore() {
        String inputColumnName = "example_column_name";
        String expectedNormalized = "EXAMPLECOLUMNNAME";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_UNDERSCORE, null);

        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveSpace() {
        String inputColumnName = "Column Name With Spaces";
        String expectedNormalized = "COLUMNNAMEWITHSPACES";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_SPACE, null);
        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_RemoveAllSpecialCharacters() {
        String inputColumnName = "Special!Characters@Here$";
        String expectedNormalized = "SPECIALCHARACTERSHERE";
        normalizer = new ColumnNameNormalizer(true, TranslationStrategy.REMOVE_ALL_SPECIAL_CHAR, null);
        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_Regex() {
        String inputColumnName = "Your @Input -String Here";
        String translationRegex = "[@-]";
        String expectedNormalized = inputColumnName.toUpperCase().replaceAll(translationRegex, "");

        ColumnNameNormalizer regexNormalizer = new ColumnNameNormalizer(true, TranslationStrategy.REGEX, translationRegex);
        String normalized = regexNormalizer.getColName(inputColumnName);

        assertEquals(expectedNormalized, normalized);
    }

    @Test
    void testNormalizingColumnName_NullInput() {
        normalizer = new ColumnNameNormalizer(true, null, null);
        String normalized = normalizer.getColName(null);

        assertNull(normalized);
    }

    @Test
    void testNormalizingColumnName_NotEnabled() {
        normalizer = new ColumnNameNormalizer(false, TranslationStrategy.REMOVE_UNDERSCORE, null);

        String inputColumnName = "example_column_name";

        String normalized = normalizer.getColName(inputColumnName);

        assertEquals(inputColumnName, normalized);
    }
}
