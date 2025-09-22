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

/**
 * NIFI Expression Language (NFEL) test patterns for comprehensive grammar testing.
 * These patterns test edge cases, autocompletion scenarios, and regression cases.
 * Used primarily for bulk grammar compliance testing.
 */

export const NFEL_PATTERNS = {
    // Basic patterns
    SIMPLE_ATTRIBUTE: '${attr}',
    SIMPLE_FUNCTION: '${uuid()}',
    SIMPLE_PARAMETER: '#{param}',
    CHAINED_FUNCTION: '${attr:toUpper()}',
    NESTED_EXPRESSION: '${filename:substring(0, ${name:length()})}',

    // String literal patterns (used in tests)
    STRING_SINGLE_QUOTE: "${attr:equals('",
    STRING_DOUBLE_QUOTE: '${attr:equals("',
    STRING_COMPLETE: '${attr:equals("test")}',

    // Expression escaping patterns
    ESCAPED_SIMPLE: '$${UserName}',
    ESCAPED_DOUBLE: '$$${attr}',
    ESCAPED_IN_TEXT: 'Price: $$50, Value: ${price}',

    // Parameter reference patterns
    PARAM_SIMPLE: '#{parameterName}',
    PARAM_WITH_DOT: '#{database.url}',
    PARAM_WITH_DASH: '#{app-config}',
    PARAM_WITH_UNDERSCORE: '#{env_var}',
    PARAM_QUOTED_SINGLE: "#{'param with spaces'}",
    PARAM_QUOTED_DOUBLE: '#{"param with spaces"}',

    // Autocompletion patterns
    INCOMPLETE_FUNCTION: '${al',
    INCOMPLETE_PARAMETER: '#{par',
    INCOMPLETE_CHAINED: '${attr:eq',
    INCOMPLETE_EMPTY_FUNCTION: '${',
    INCOMPLETE_EMPTY_PARAMETER: '#{',

    // Complex patterns
    MULTIPLE_CHAINING: "${attr:equals('value'):contains('test')}",
    EMBEDDED_PARAMETER: '${#{param}:toUpper()}',
    MIXED_CONTENT: 'Hello ${name}, you have ${count} items.',
    COMPLEX_NESTED: '${filename:replace(${attr:substring(0, 3)}, "new")}',

    // Enhanced grammar features
    COMMENT_SIMPLE: '# This is a comment\n${attr}',
    SEMICOLON_IN_STRING: '${attr:equals("test;value")}',
    SEMICOLON_AS_SEPARATOR: '${attr}; ${other}',

    // Edge cases for regression testing
    MALFORMED_UNCLOSED: '${unclosed',
    MALFORMED_MISSING: '${:missing}',
    MALFORMED_EMPTY_FUNCTION: '${attr:}',
    MALFORMED_NESTED: '${nested${broken}',

    // File processing patterns
    FILE_EXTENSION: '${filename:substring(${filename:lastIndexOf("."):plus(1)})}',
    FILE_WITHOUT_EXTENSION: '${filename:substring(0, ${filename:lastIndexOf(".")})}',

    // Date formatting patterns
    CURRENT_DATE: '${now():format("yyyy-MM-dd")}',
    TIMESTAMP: '${now():format("yyyy-MM-dd HH:mm:ss.SSS")}',

    // JSON processing patterns
    JSON_EXTRACT: '${json:jsonPath("$.user.id")}',
    JSON_SET: '${json:jsonPathSet("$.processed", "true")}',

    // Conditional patterns
    DEFAULT_IF_EMPTY: '${value:isEmpty():ifElse(#{defaultValue}, ${value})}',
    NULL_HANDLING: '${text:isNull():not():ifElse(${text:toUpper()}, "")}',

    // Performance patterns
    LARGE_CONTENT_HASH: '${largeText:substring(0, 1000):hash("MD5")}',

    // Boolean logic patterns
    BOOLEAN_AND: '${size:gt("1000"):and(${type:equals("file")})}',
    BOOLEAN_OR: '${status:equals("success"):or(${retry:lt("3")})}',

    // Math operations
    MATH_COMPLEX: '${value1:plus(${value2:multiply(10)})}',
    MATH_DIVISION: '${total:divide(${count:toNumber()})}',

    // Text manipulation
    TEXT_NORMALIZE: '${text:trim():replaceAll("\\s+", " ")}',
    TEXT_CLEAN_FILENAME: '${filename:replaceAll("[^a-zA-Z0-9._-]", "_")}',

    // Encoding patterns
    HASH_SHA256: '${content:hash("SHA-256")}',
    URL_ENCODE: '${text:urlEncode()}',
    BASE64_DECODE: '${encoded:base64Decode()}',

    // Multi-attribute functions
    ANY_ATTRIBUTE: '${anyAttribute("user.*")}',
    ALL_ATTRIBUTES: '${allAttributes("system.*")}',

    // URI operations
    GET_URI: '${getUri("https", "example.com", 443, "/api", "param=value", "user", "pass")}',

    // Regular expressions
    REGEX_FIND: '${text:find("\\d+")}',
    REGEX_REPLACE: '${content:replaceByPattern("\\s+", " ")}',

    // State operations
    GET_STATE: '${getStateValue("counter")}',

    // Thread information
    THREAD_INFO: '${thread()}',

    // Delineated values
    DELINEATED_ANY: '${anyDelineatedValue(",", 2)}',
    DELINEATED_ALL: '${allDelineatedValues("|")}',

    // Data validation patterns
    EMAIL_VALIDATION: '${email:matches("^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$")}',
    PHONE_VALIDATION: '${phone:matches("^\\+?[1-9]\\d{1,14}$")}',

    // String escaping patterns
    ESCAPE_JSON: '${text:escapeJson()}',
    ESCAPE_HTML: '${html:escapeHtml4()}',
    ESCAPE_CSV: '${csv:escapeCsv()}',

    // Enhanced string literals with escapes
    STRING_ESCAPE_QUOTES: "${attr:equals('He said \\'hello\\'')}",
    STRING_ESCAPE_NEWLINE: '${attr:equals("Line 1\\nLine 2")}',
    STRING_ESCAPE_TAB: '${attr:equals("Column 1\\tColumn 2")}',
    STRING_ESCAPE_BACKSLASH: '${attr:equals("Path: C:\\\\folder")}',

    // Patterns that should be allowed (incomplete for testing)
    INCOMPLETE_STRING_SINGLE: "${attr:equals('test",
    INCOMPLETE_STRING_DOUBLE: '${attr:equals("test',
    INCOMPLETE_QUOTED_PARAM_SINGLE: "#{'param",
    INCOMPLETE_QUOTED_PARAM_DOUBLE: '#{"param',

    // Mixed content with comments
    COMMENT_MIXED: 'Hello # This is a comment\n${name} World',
    COMMENT_AFTER_EXPR: '${attr} # This is a comment\nNext line',
    COMMENT_MULTILINE: '# Comment 1\n# Comment 2\n${attr}',

    // Complex real-world example
    COMPLEX_REAL_WORLD:
        '${filename:substring(0, ${filename:lastIndexOf(".")}):toUpper():prepend(#{prefix}):append("_PROCESSED")}',

    // Error handling with defaults
    ERROR_HANDLING: '${jsonData:jsonPath("$.user.email"):replaceNull("noemail@example.com")}',

    // Multiple nested expressions
    MULTI_NESTED_EXPRESSIONS: '${attr:substring(${start}, ${end})}',
    MIXED_PARAM_EXPR_NESTING: '${attr:replace(#{search}, ${replacement:toUpper()})}',
    DEEP_NESTING_WITH_CONVERSION: '${attr:contains(${other:substring(${start:toNumber()}, 5)})}',
    COMPLEX_PATH_MANIPULATION: '${path:replace(${dir:append("/")}${file:substring(0, ${len:toNumber()})}, ".txt")}',

    // Incomplete expression edge cases
    INCOMPLETE_CHAINED_FUNCTION: '${attr:toUpper():',
    INCOMPLETE_MULTI_CHAINED: '${attr:trim():toUpper():',
    INCOMPLETE_QUOTED_PARAM_PARTIAL: '#{"',
    INCOMPLETE_QUOTED_PARAM_SINGLE_PARTIAL: "#{'myParam",
    INCOMPLETE_WITH_TRAILING_TEXT: '${attr:toUpper()} more text'
};
