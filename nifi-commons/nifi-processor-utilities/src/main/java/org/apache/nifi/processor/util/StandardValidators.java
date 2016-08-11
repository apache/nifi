/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processor.util;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.util.FormatUtils;

public class StandardValidators {

    //
    //
    // STATICALLY DEFINED VALIDATORS
    //
    //
    public static final Validator ATTRIBUTE_KEY_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject(subject).input(input);
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            try {
                FlowFile.KeyValidator.validateKey(input);
                builder.valid(true);
            } catch (final IllegalArgumentException e) {
                builder.valid(false).explanation(e.getMessage());
            }

            return builder.build();
        }
    };

    public static final Validator ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            builder.subject("Property Name").input(subject);
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return builder.valid(true).explanation("Contains Expression Language").build();
            }

            try {
                FlowFile.KeyValidator.validateKey(subject);
                builder.valid(true);
            } catch (final IllegalArgumentException e) {
                builder.valid(false).explanation(e.getMessage());
            }

            return builder.build();
        }
    };

    public static final Validator POSITIVE_INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                final int intVal = Integer.parseInt(value);

                if (intVal <= 0) {
                    reason = "not a positive value";
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator POSITIVE_LONG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                final long longVal = Long.parseLong(value);

                if (longVal <= 0) {
                    reason = "not a positive value";
                }
            } catch (final NumberFormatException e) {
                reason = "not a valid 64-bit integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator PORT_VALIDATOR = createLongValidator(1, 65535, true);

    /**
     * {@link Validator} that ensures that value's length > 0
     */
    public static final Validator NON_EMPTY_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            return new ValidationResult.Builder().subject(subject).input(value).valid(value != null && !value.isEmpty()).explanation(subject + " cannot be empty").build();
        }
    };

    /**
     * {@link Validator} that ensures that value has 1+ non-whitespace
     * characters
     */
    public static final Validator NON_BLANK_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            return new ValidationResult.Builder().subject(subject).input(value)
                    .valid(value != null && !value.trim().isEmpty())
                    .explanation(subject
                    + " must contain at least one character that is not white space").build();
        }
    };

    public static final Validator BOOLEAN_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            final boolean valid = "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
            final String explanation = valid ? null : "Value must be 'true' or 'false'";
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    };

    public static final Validator INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                Integer.parseInt(value);
            } catch (final NumberFormatException e) {
                reason = "not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator LONG_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                Long.parseLong(value);
            } catch (final NumberFormatException e) {
                reason = "not a valid Long";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator NON_NEGATIVE_INTEGER_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                final int intVal = Integer.parseInt(value);

                if (intVal < 0) {
                    reason = "value is negative";
                }
            } catch (final NumberFormatException e) {
                reason = "value is not a valid integer";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    public static final Validator CHARACTER_SET_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                final ResultType resultType = context.newExpressionLanguageCompiler().getResultType(value);
                if (!resultType.equals(ResultType.STRING)) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(value)
                            .valid(false)
                            .explanation("Expected Attribute Query to return type " + ResultType.STRING + " but query returns type " + resultType)
                            .build();
                }

                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            String reason = null;
            try {
                if (!Charset.isSupported(value)) {
                    reason = "Character Set is not supported by this JVM.";
                }
            } catch (final UnsupportedCharsetException uce) {
                reason = "Character Set is not supported by this JVM.";
            } catch (final IllegalArgumentException iae) {
                reason = "Character Set value cannot be null.";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    /**
     * URL Validator that does not allow the Expression Language to be used
     */
    public static final Validator URL_VALIDATOR = createURLValidator();

    public static final Validator URI_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            try {
                new URI(input);
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid URI").valid(true).build();
            } catch (final Exception e) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Not a valid URI").valid(false).build();
            }
        }
    };

    public static final Validator REGULAR_EXPRESSION_VALIDATOR = createRegexValidator(0, Integer.MAX_VALUE, false);

    public static final Validator ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                try {
                    final String result = context.newExpressionLanguageCompiler().validateExpression(input, true);
                    if (!StringUtils.isEmpty(result)) {
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(result).build();
                    }
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(e.getMessage()).build();
                }
            }

            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }

    };

    public static final Validator TIME_PERIOD_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (input == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Time Period cannot be null").build();
            }
            if (Pattern.compile(FormatUtils.TIME_DURATION_REGEX).matcher(input.toLowerCase()).matches()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Must be of format <duration> <TimeUnit> where <duration> is a "
                                + "non-negative integer and TimeUnit is a supported Time Unit, such "
                                + "as: nanos, millis, secs, mins, hrs, days")
                        .build();
            }
        }
    };

    public static final Validator DATA_SIZE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (input == null) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Data Size cannot be null")
                        .build();
            }
            if (Pattern.compile(DataUnit.DATA_SIZE_REGEX).matcher(input.toUpperCase()).matches()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } else {
                return new ValidationResult.Builder()
                        .subject(subject).input(input)
                        .valid(false)
                        .explanation("Must be of format <Data Size> <Data Unit> where <Data Size>"
                                + " is a non-negative integer and <Data Unit> is a supported Data"
                                + " Unit, such as: B, KB, MB, GB, TB")
                        .build();
            }
        }
    };

    public static final Validator FILE_EXISTS_VALIDATOR = new FileExistsValidator(true);

    //
    //
    // FACTORY METHODS FOR VALIDATORS
    //
    //
    public static Validator createDirectoryExistsValidator(final boolean allowExpressionLanguage, final boolean createDirectoryIfMissing) {
        return new DirectoryExistsValidator(allowExpressionLanguage, createDirectoryIfMissing);
    }

    private static Validator createURLValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
                }

                try {
                    final String evaluatedInput = context.newPropertyValue(input).evaluateAttributeExpressions().getValue();
                    new URL(evaluatedInput);
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Valid URL").valid(true).build();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Not a valid URL").valid(false).build();
                }
            }
        };
    }

    public static Validator createTimePeriodValidator(final long minTime, final TimeUnit minTimeUnit, final long maxTime, final TimeUnit maxTimeUnit) {
        return new TimePeriodValidator(minTime, minTimeUnit, maxTime, maxTimeUnit);
    }

    public static Validator createAttributeExpressionLanguageValidator(final ResultType expectedResultType) {
        return createAttributeExpressionLanguageValidator(expectedResultType, true);
    }

    public static Validator createDataSizeBoundsValidator(final long minBytesInclusive, final long maxBytesInclusive) {
        return new Validator() {

            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
                }

                final ValidationResult vr = DATA_SIZE_VALIDATOR.validate(subject, input, context);
                if (!vr.isValid()) {
                    return vr;
                }
                final long dataSizeBytes = DataUnit.parseDataSize(input, DataUnit.B).longValue();
                if (dataSizeBytes < minBytesInclusive) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Cannot be smaller than " + minBytesInclusive + " bytes").build();
                }
                if (dataSizeBytes > maxBytesInclusive) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Cannot be larger than " + maxBytesInclusive + " bytes").build();
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };

    }

    public static Validator createRegexMatchingValidator(final Pattern pattern) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
                }

                final boolean matches = pattern.matcher(input).matches();
                return new ValidationResult.Builder()
                        .input(input)
                        .subject(subject)
                        .valid(matches)
                        .explanation(matches ? null : "Value does not match regular expression: " + pattern.pattern())
                        .build();
            }
        };
    }

    /**
     * Creates a @{link Validator} that ensure that a value is a valid Java
     * Regular Expression with at least <code>minCapturingGroups</code>
     * capturing groups and at most <code>maxCapturingGroups</code> capturing
     * groups. If <code>supportAttributeExpressionLanguage</code> is set to
     * <code>true</code>, the value may also include the Expression Language,
     * but the result of evaluating the Expression Language will be applied
     * before the Regular Expression is performed. In this case, the Expression
     * Language will not support FlowFile Attributes but only System/JVM
     * Properties
     *
     * @param minCapturingGroups minimum capturing groups allowed
     * @param maxCapturingGroups maximum capturing groups allowed
     * @param supportAttributeExpressionLanguage whether or not to support
     * expression language
     * @return validator
     */
    public static Validator createRegexValidator(final int minCapturingGroups, final int maxCapturingGroups, final boolean supportAttributeExpressionLanguage) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
                try {
                    final String substituted;
                    if (supportAttributeExpressionLanguage) {
                        try {
                            substituted = context.newPropertyValue(value).evaluateAttributeExpressions().getValue();
                        } catch (final Exception e) {
                            return new ValidationResult.Builder()
                                    .subject(subject)
                                    .input(value)
                                    .valid(false)
                                    .explanation("Failed to evaluate the Attribute Expression Language due to " + e.toString())
                                    .build();
                        }
                    } else {
                        substituted = value;
                    }

                    final Pattern pattern = Pattern.compile(substituted);
                    final int numGroups = pattern.matcher("").groupCount();
                    if (numGroups < minCapturingGroups || numGroups > maxCapturingGroups) {
                        return new ValidationResult.Builder()
                                .subject(subject)
                                .input(value)
                                .valid(false)
                                .explanation("RegEx is required to have between " + minCapturingGroups + " and " + maxCapturingGroups + " Capturing Groups but has " + numGroups)
                                .build();
                    }

                    return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
                } catch (final Exception e) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(value)
                            .valid(false)
                            .explanation("Not a valid Java Regular Expression")
                            .build();
                }

            }
        };
    }

    public static Validator createAttributeExpressionLanguageValidator(final ResultType expectedResultType, final boolean allowExtraCharacters) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                final String syntaxError = context.newExpressionLanguageCompiler().validateExpression(input, allowExtraCharacters);
                if (syntaxError != null) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(syntaxError).build();
                }

                final ResultType resultType = allowExtraCharacters ? ResultType.STRING : context.newExpressionLanguageCompiler().getResultType(input);
                if (!resultType.equals(expectedResultType)) {
                    return new ValidationResult.Builder()
                            .subject(subject)
                            .input(input)
                            .valid(false)
                            .explanation("Expected Attribute Query to return type " + expectedResultType + " but query returns type " + resultType)
                            .build();
                }

                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };
    }

    public static Validator createLongValidator(final long minimum, final long maximum, final boolean inclusive) {
        return new Validator() {
            @Override
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                    return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
                }

                String reason = null;
                try {
                    final long longVal = Long.parseLong(input);
                    if (longVal < minimum || (!inclusive && longVal == minimum) | longVal > maximum || (!inclusive && longVal == maximum)) {
                        reason = "Value must be between " + minimum + " and " + maximum + " (" + (inclusive ? "inclusive" : "exclusive") + ")";
                    }
                } catch (final NumberFormatException e) {
                    reason = "not a valid integer";
                }

                return new ValidationResult.Builder().subject(subject).input(input).explanation(reason).valid(reason == null).build();
            }

        };
    }

    //
    //
    // SPECIFIC VALIDATOR IMPLEMENTATIONS THAT CANNOT BE ANONYMOUS CLASSES
    //
    //
    static class TimePeriodValidator implements Validator {

        private final Pattern pattern;

        private final long minNanos;
        private final long maxNanos;

        private final String minValueEnglish;
        private final String maxValueEnglish;

        public TimePeriodValidator(final long minValue, final TimeUnit minTimeUnit, final long maxValue, final TimeUnit maxTimeUnit) {
            pattern = Pattern.compile(FormatUtils.TIME_DURATION_REGEX);

            this.minNanos = TimeUnit.NANOSECONDS.convert(minValue, minTimeUnit);
            this.maxNanos = TimeUnit.NANOSECONDS.convert(maxValue, maxTimeUnit);
            this.minValueEnglish = minValue + " " + minTimeUnit.toString();
            this.maxValueEnglish = maxValue + " " + maxTimeUnit.toString();
        }

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            if (input == null) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("Time Period cannot be null").build();
            }
            final String lowerCase = input.toLowerCase();
            final boolean validSyntax = pattern.matcher(lowerCase).matches();
            final ValidationResult.Builder builder = new ValidationResult.Builder();
            if (validSyntax) {
                final long nanos = FormatUtils.getTimeDuration(lowerCase, TimeUnit.NANOSECONDS);

                if (nanos < minNanos || nanos > maxNanos) {
                    builder.subject(subject).input(input).valid(false)
                            .explanation("Must be in the range of " + minValueEnglish + " to " + maxValueEnglish);
                } else {
                    builder.subject(subject).input(input).valid(true);
                }
            } else {
                builder.subject(subject).input(input).valid(false)
                        .explanation("Must be of format <duration> <TimeUnit> where <duration> is a non-negative "
                                + "integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days");
            }
            return builder.build();
        }
    }

    public static class FileExistsValidator implements Validator {

        private final boolean allowEL;

        public FileExistsValidator(final boolean allowExpressionLanguage) {
            this.allowEL = allowExpressionLanguage;
        }

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            final String substituted;
            if (allowEL) {
                try {
                    substituted = context.newPropertyValue(value).evaluateAttributeExpressions().getValue();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(value).valid(false)
                            .explanation("Not a valid Expression Language value: " + e.getMessage()).build();
                }
            } else {
                substituted = value;
            }

            final File file = new File(substituted);
            final boolean valid = file.exists();
            final String explanation = valid ? null : "File " + file + " does not exist";
            return new ValidationResult.Builder().subject(subject).input(value).valid(valid).explanation(explanation).build();
        }
    }

    public static class StringLengthValidator implements Validator {
        private final int minimum;
        private final int maximum;

        public StringLengthValidator(int minimum, int maximum) {
            this.minimum = minimum;
            this.maximum = maximum;
        }

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (value.length() < minimum || value.length() > maximum) {
                return new ValidationResult.Builder()
                  .subject(subject)
                  .valid(false)
                  .input(value)
                  .explanation(String.format("String length invalid [min: %d, max: %d]", minimum, maximum))
                  .build();
            } else {
                return new ValidationResult.Builder()
                  .valid(true)
                  .input(value)
                  .subject(subject)
                  .build();
            }
        }
    }

    public static class DirectoryExistsValidator implements Validator {

        private final boolean allowEL;
        private final boolean create;

        public DirectoryExistsValidator(final boolean allowExpressionLanguage, final boolean create) {
            this.allowEL = allowExpressionLanguage;
            this.create = create;
        }

        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
            }

            final String substituted;
            if (allowEL) {
                try {
                    substituted = context.newPropertyValue(value).evaluateAttributeExpressions().getValue();
                } catch (final Exception e) {
                    return new ValidationResult.Builder().subject(subject).input(value).valid(false)
                            .explanation("Not a valid Expression Language value: " + e.getMessage()).build();
                }

                if (substituted.trim().isEmpty() && !value.trim().isEmpty()) {
                    // User specified an Expression and nothing more... assume valid.
                    return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
                }
            } else {
                substituted = value;
            }

            String reason = null;
            try {
                final File file = new File(substituted);
                if (!file.exists()) {
                    if (!create) {
                        reason = "Directory does not exist";
                    } else if (!file.mkdirs()) {
                        reason = "Directory does not exist and could not be created";
                    }
                } else if (!file.isDirectory()) {
                    reason = "Path does not point to a directory";
                }
            } catch (final Exception e) {
                reason = "Value is not a valid directory name";
            }

            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    }

}
