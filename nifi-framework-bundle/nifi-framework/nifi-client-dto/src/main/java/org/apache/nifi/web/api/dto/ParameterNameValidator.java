package org.apache.nifi.web.api.dto;

import java.util.regex.Pattern;

public final class ParameterNameValidator {
    private static final Pattern VALID_PARAMETER_NAME_PATTERN = Pattern.compile("[A-Za-z0-9 ._\\-]+");

    private ParameterNameValidator() {
    }

    public static void validate(final String parameterName) {
        if (parameterName == null || !VALID_PARAMETER_NAME_PATTERN.matcher(parameterName).matches()) {
            throw new IllegalArgumentException("Request contains an illegal Parameter Name (" + parameterName
                    + "). Parameter names may only include letters, numbers, spaces, and the special characters .-_");
        }
    }
}
