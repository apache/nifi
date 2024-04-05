package org.apache.nifi.processors.standard.db;

import org.apache.nifi.processors.standard.db.impl.PatternNormalizer;
import org.apache.nifi.processors.standard.db.impl.RemoveAllSpecialCharNormalizer;
import org.apache.nifi.processors.standard.db.impl.RemoveSpaceNormalizer;
import org.apache.nifi.processors.standard.db.impl.RemoveUnderscoreNormalizer;

import java.util.regex.Pattern;

public class ColumnNameNormalizerFactory {
    public static ColumnNameNormalizer getNormalizer(TranslationStrategy strategy, Pattern regex) {

        return switch (strategy) {
            case REMOVE_UNDERSCORE -> new RemoveUnderscoreNormalizer();
            case REMOVE_SPACE -> new RemoveSpaceNormalizer();
            case REMOVE_ALL_SPECIAL_CHAR -> new RemoveAllSpecialCharNormalizer();
            case PATTERN -> new PatternNormalizer(regex);
        };
    }
}
