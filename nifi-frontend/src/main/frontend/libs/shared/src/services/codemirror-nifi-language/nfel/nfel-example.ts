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
 * Examples of NIFI Expression Language (NFEL) expressions for testing and demonstration.
 * These examples are based on the ANTLR grammar structure.
 */

export const NFEL_ANTLR_EXAMPLES = [
    // Basic attribute reference
    '${filename}',

    // String functions - zero arguments
    '${filename:toUpper()}',
    '${content:trim()}',
    '${data:base64Encode()}',

    // String functions - one argument
    '${filename:substring(5)}',
    '${content:replace("old", "new")}',
    '${data:contains("pattern")}',

    // String functions - two arguments
    '${text:replaceAll("\\s+", " ")}',
    '${date:format("yyyy-MM-dd")}',
    '${value:padLeft(10, "0")}',

    // Boolean functions
    '${filename:isEmpty()}',
    '${size:equals("100")}',
    '${content:matches(".*pattern.*")}',

    // Number functions
    '${content:length()}',
    '${value:toNumber()}',
    '${num1:plus(${num2})}',

    // Chained functions
    '${filename:toUpper():substring(0, 5)}',
    '${content:trim():replace(" ", "_"):toLowerCase()}',

    // Nested expressions
    '${filename:substring(0, ${name:length()})}',
    '${content:replace(${oldValue}, ${newValue})}',

    // Parameter references - should be recognized as AttributeName.ParameterName, NOT FunctionName
    '#{parameterName}', // Simple parameter name
    '#{database.url}', // Parameter with dot
    '#{config.timeout}', // Parameter with dot
    '#{user.settings}', // Parameter with dot
    '#{app-config}', // Parameter with dash
    '#{env_var}', // Parameter with underscore

    // Mixed expressions with parameters
    '${filename:prepend(#{prefix})}',
    '${content:replace("placeholder", #{replacement})}',

    // Multi-attribute functions
    '${anyAttribute("user.*")}',
    '${allAttributes("system.*")}',
    '${anyMatchingAttribute("temp.*", "backup.*")}',

    // Standalone functions
    '${uuid()}',
    '${now()}',
    '${hostname()}',
    '${ip()}',
    '${random()}',

    // Complex expressions
    '${filename:substring(${path:lastIndexOf("/"):plus(1)})}',
    '${content:jsonPath("$.user.name"):replaceNull("Unknown")}',

    // Boolean logic
    '${size:gt("1000"):and(${type:equals("file")})}',
    '${status:equals("success"):or(${retry:lt("3")})}',

    // Date/time functions
    '${timestamp:toDate("yyyy-MM-dd HH:mm:ss")}',
    '${created:formatInstant("ISO_INSTANT")}',

    // Math operations
    '${value1:plus(${value2:multiply(10)})}',
    '${total:divide(${count:toNumber()})}',

    // Conditional logic
    '${status:ifElse("success", "OK", "ERROR")}',
    '${value:replaceNull("default")}',

    // JSON operations
    '${json:jsonPath("$.items[0].name")}',
    '${data:jsonPathSet("$.status", "processed")}',

    // URI operations
    '${getUri("https", "example.com", 443, "/api", "param=value", "user", "pass")}',

    // Hash and encoding
    '${content:hash("SHA-256")}',
    '${text:urlEncode()}',
    '${encoded:base64Decode()}',

    // Text manipulation
    '${text:escapeJson()}',
    '${html:escapeHtml4()}',
    '${csv:escapeCsv()}',

    // Delineated values
    '${anyDelineatedValue(",", 2)}',
    '${allDelineatedValues("|")}',

    // Regular expressions
    '${text:find("\\d+")}',
    '${content:replaceByPattern("\\s+", " ")}',

    // State operations
    '${getStateValue("counter")}',

    // Thread information
    '${thread()}',

    // Null handling
    '${value:notNull()}',
    '${data:isNull():not()}',

    // Multiple conditions
    '${type:in("file", "directory", "link")}',

    // Complex nested example
    '${filename:substring(0, ${filename:lastIndexOf(".")}):toUpper():prepend(#{prefix}):append("_PROCESSED")}',

    // Error handling with defaults
    '${jsonData:jsonPath("$.user.email"):replaceNull("noemail@example.com")}',

    // Performance optimization
    '${largeText:substring(0, 1000):hash("MD5")}'
];

/**
 * Common patterns and use cases for NFEL expressions
 */
export const NFEL_PATTERNS = {
    // File processing
    FILE_EXTENSION: '${filename:substring(${filename:lastIndexOf("."):plus(1)})}',
    FILE_WITHOUT_EXTENSION: '${filename:substring(0, ${filename:lastIndexOf(".")})}',

    // Date formatting
    CURRENT_DATE: '${now():format("yyyy-MM-dd")}',
    TIMESTAMP: '${now():format("yyyy-MM-dd HH:mm:ss.SSS")}',

    // Data validation
    IS_EMAIL: '${email:matches("^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$")}',
    IS_PHONE: '${phone:matches("^\\+?[1-9]\\d{1,14}$")}',

    // Text normalization
    NORMALIZE_WHITESPACE: '${text:trim():replaceAll("\\s+", " ")}',
    CLEAN_FILENAME: '${filename:replaceAll("[^a-zA-Z0-9._-]", "_")}',

    // JSON processing
    EXTRACT_USER_ID: '${json:jsonPath("$.user.id"):toNumber()}',
    SET_PROCESSED_FLAG: '${json:jsonPathSet("$.processed", "true")}',

    // Conditional processing
    DEFAULT_IF_EMPTY: '${value:isEmpty():ifElse(#{defaultValue}, ${value})}',
    UPPER_IF_NOT_NULL: '${text:isNull():not():ifElse(${text:toUpper()}, "")}',

    // Parameter reference tests (should show ParameterName, not FunctionName)
    PARAMETER_TEST_1: '#{length}', // 'length' should be ParameterName
    PARAMETER_TEST_2: '#{count}', // 'count' should be ParameterName
    PARAMETER_TEST_3: '#{database.url}', // complex parameter name
    PARAMETER_TEST_4: '#{app-config}', // parameter with dash
    PARAMETER_TEST_5: '#{env_var}', // parameter with underscore

    // Function call tests (should show FunctionName)
    FUNCTION_TEST_1: '${length()}', // 'length' should be FunctionName
    FUNCTION_TEST_2: '${count()}', // 'count' should be FunctionName

    // Embedded parameter tests (ParameterReference inside Expression)
    EMBEDDED_PARAM_1: '${#{param}}', // 'param' should be ParameterName, not AttributeName
    EMBEDDED_PARAM_2: '${value:ifElse(#{defaultValue}, "empty")}', // 'defaultValue' should be ParameterName

    // Function name tests (should be FunctionName, not AttributeName)
    FUNCTION_NAME_TEST_1: '${text:equals("test")}', // 'equals' should be FunctionName (chained)
    FUNCTION_NAME_TEST_2: '${ip()}', // 'ip' should be FunctionName (standalone)
    FUNCTION_NAME_TEST_3: '${attr:equals("asdf")}', // 'equals' should be FunctionName (user's example)

    // Contextual tests - this is the critical test
    CONTEXTUAL_TEST: '#{param}', // 'param' should be ParameterName, NOT FunctionName

    // Comprehensive contextual tests
    ATTRIBUTE_TEST: '${attr}', // 'attr' should be AttributeName
    FUNCTION_CONTEXT_TEST: '${ip()}', // 'ip' should be FunctionName
    PARAMETER_CONTEXT_TEST: '#{param}', // 'param' should be ParameterName
    MIXED_CONTEXT_TEST: '${attr:equals(#{param})}', // 'attr'→AttributeName, 'equals'→FunctionName, 'param'→ParameterName

    // Chained function test - this was the reported issue
    CHAINED_FUNCTION_TEST: '${attr:equals("asdf")}', // 'equals' should be FunctionName, not cause grammar error

    // Standalone vs Chained function distinction tests
    STANDALONE_IP: '${ip()}', // 'ip' should be identifier.StandaloneFunctionName
    STANDALONE_UUID: '${UUID()}', // 'UUID' should be identifier.StandaloneFunctionName
    STANDALONE_NOW: '${now()}', // 'now' should be identifier.StandaloneFunctionName
    CHAINED_EQUALS: '${attr:equals("test")}', // 'equals' should be identifier.FunctionName (chained)
    CHAINED_LENGTH: '${text:length()}', // 'length' should be identifier.FunctionName (chained)
    CHAINED_UPPER: '${text:toUpper()}', // 'toUpper' should be identifier.FunctionName (chained)

    // Simple tests for debugging node types
    SIMPLE_STANDALONE: '${ip()}', // Should parse as StandaloneFunction > standaloneFunctionName > identifier.StandaloneFunctionName
    SIMPLE_CHAINED: '${a:b()}', // Should parse as AttributeRef + FunctionCall > functionName > identifier.FunctionName
    SIMPLE_ATTRIBUTE: '${attr}', // Should parse as AttributeRef > attributeName > identifier.AttributeName
    SIMPLE_PARAMETER: '#{param}', // Should parse as ParameterReference > parameterName > identifier.ParameterName

    // Incomplete expression tests for autocompletion
    INCOMPLETE_STANDALONE: '${al', // Should detect as incomplete standalone function and offer completions starting with 'al'
    INCOMPLETE_PARAMETER: '#{par', // Should detect as incomplete parameter and offer completions starting with 'par'
    INCOMPLETE_CHAINED: '${attr:eq', // Should detect as incomplete chained function and offer completions starting with 'eq'
    INCOMPLETE_EMPTY_STANDALONE: '${', // Should detect as incomplete standalone function
    INCOMPLETE_EMPTY_PARAMETER: '#{', // Should detect as incomplete parameter

    // String literal tests - should NOT offer autocompletion (handled by service logic)
    STRING_SINGLE_QUOTE: "${attr:equals('", // Should detect unclosed single quote and suppress completions
    STRING_DOUBLE_QUOTE: '${attr:equals("', // Should detect unclosed double quote and suppress completions
    STRING_PARTIAL_SINGLE: "${attr:equals('partial", // Should detect unclosed single quote
    STRING_PARTIAL_DOUBLE: '${attr:equals("partial', // Should detect unclosed double quote
    STRING_NESTED_PARAM: '${attr:equals("#{', // Should detect unclosed double quote (parameter syntax inside string)
    STRING_COMPLETE_SINGLE: "${attr:equals('test')}", // Should parse as StringLiteral and allow completions outside
    STRING_COMPLETE_DOUBLE: '${attr:equals("test")}', // Should parse as StringLiteral and allow completions outside
    STRING_ESCAPED_QUOTE: "${attr:equals('te\\'st", // Should handle escaped quotes correctly

    // Expression escaping tests - should parse as EscapedDollar + Text
    ESCAPED_EXPRESSION_SIMPLE: '$${UserName}', // Should render as literal "${UserName}"
    ESCAPED_EXPRESSION_DOUBLE: '$$${attr}', // Should render as "$" + value of attr
    ESCAPED_EXPRESSION_QUADRUPLE: '$$$${attr}', // Should render as "$${attr}"
    ESCAPED_EXPRESSION_IN_TEXT: 'Hello $${UserName} world', // Should have Text + EscapedDollar + Text structure
    ESCAPED_EXPRESSION_MIXED: 'Price: $$50, Value: ${price}', // Should mix escaped and real expressions

    // Autocompletion escaping tests - testing when autocomplete should/shouldn't appear
    AUTOCOMPLETE_VALID_SINGLE: '${', // 1 $ -> should show autocomplete
    AUTOCOMPLETE_ESCAPED_DOUBLE: '$${', // 2 $ -> should NOT show autocomplete (escaped)
    AUTOCOMPLETE_VALID_TRIPLE: '$$${', // 3 $ -> should show autocomplete (escaped $ + valid expression)
    AUTOCOMPLETE_ESCAPED_QUAD: '$$$${', // 4 $ -> should NOT show autocomplete (escaped)
    AUTOCOMPLETE_VALID_FIVE: '$$$$${', // 5 $ -> should show autocomplete
    AUTOCOMPLETE_ESCAPED_SIX: '$$$$$${', // 6 $ -> should NOT show autocomplete (escaped)

    // Quoted parameter name tests - should allow autocompletion inside quotes for parameter names
    QUOTED_PARAM_SINGLE_EMPTY: "#{'", // Should show parameter autocompletion
    QUOTED_PARAM_DOUBLE_EMPTY: '#{"', // Should show parameter autocompletion
    QUOTED_PARAM_SINGLE_PARTIAL: "#{'my par", // Should show parameter autocompletion with 'my par' prefix
    QUOTED_PARAM_DOUBLE_PARTIAL: '#{"my par', // Should show parameter autocompletion with 'my par' prefix
    QUOTED_PARAM_SINGLE_COMPLETE: "#{'my parameter'}", // Complete quoted parameter
    QUOTED_PARAM_DOUBLE_COMPLETE: '#{"my parameter"}', // Complete quoted parameter
    QUOTED_PARAM_WITH_SPACES: "#{'param with spaces", // Should handle spaces in parameter names
    QUOTED_PARAM_WITH_SPECIAL: '#{"param-with.special_chars', // Should handle special characters

    // Edge cases for quoted parameters
    QUOTED_PARAM_MIXED_QUOTES: "#{\"param with 'nested' quotes", // Should handle nested quotes
    QUOTED_PARAM_EMPTY_STRING: '#{"', // Should handle empty quoted parameter
    QUOTED_PARAM_JUST_SPACE: '#{"   ', // Should handle whitespace-only parameter
    QUOTED_PARAM_ERROR_CONTEXT: '#{unclosed', // Should detect parameter context even with parse errors

    // Embedded parameter chained function tests - should allow chained function autocompletion
    EMBEDDED_PARAM_CHAINED_COMPLETE: "${#{param}:equals('value')}", // Complete embedded parameter with chained function
    EMBEDDED_PARAM_CHAINED_INCOMPLETE: '${#{param}:eq', // Should show chained function autocompletion for 'eq'
    EMBEDDED_PARAM_CHAINED_EMPTY: '${#{param}:', // Should show all chained function options
    EMBEDDED_PARAM_QUOTED_CHAINED: '${#{"param name"}:eq', // Should work with quoted parameter names
    EMBEDDED_PARAM_MULTIPLE_CHAINS: "${#{param}:toUpper():contains('test')}", // Multiple chained functions
    EMBEDDED_PARAM_NESTED_EXPR: '${#{param}:equals(${other})}', // Nested expressions in function arguments

    // Multiple function chaining tests - should support chaining multiple functions
    MULTIPLE_CHAIN_SIMPLE: "${attr:equals('value'):equals(true)}", // Complete double chaining
    MULTIPLE_CHAIN_INCOMPLETE: "${attr:equals('value'):eq", // Should show chained function autocompletion for 'eq'
    MULTIPLE_CHAIN_EMPTY: "${attr:equals('value'):", // Should show all chained function options after first chain
    MULTIPLE_CHAIN_TRIPLE: "${attr:toUpper():contains('TEST'):equals(true)}", // Triple chaining
    MULTIPLE_CHAIN_WITH_PARAM: "${#{param}:toUpper():contains('test'):eq", // Multiple chains with embedded parameter
    MULTIPLE_CHAIN_COMPLEX_ARGS: "${attr:replace('old', 'new'):toUpper():startsWith('NEW'):eq", // Complex arguments with multiple chains
    MULTIPLE_CHAIN_NESTED: '${attr:equals(${other:toUpper()}):equals(true)}', // Nested expressions in chained functions

    // String literal escape sequences tests
    STRING_ESCAPE_QUOTES: "${attr:equals('He said \\'hello\\'')}", // Should handle escaped quotes
    STRING_ESCAPE_NEWLINE: '${attr:equals("Line 1\\nLine 2")}', // Should handle \n escape
    STRING_ESCAPE_TAB: '${attr:equals("Column 1\\tColumn 2")}', // Should handle \t escape
    STRING_ESCAPE_BACKSLASH: '${attr:equals("Path: C:\\\\folder")}', // Should handle \\ escape
    STRING_ESCAPE_MIXED: '${attr:equals("Quote: \\"text\\", New: \\n, Tab: \\t")}', // Should handle multiple escapes

    // Enhanced string literal tests (new grammar features)
    STRING_ENHANCED_ESCAPE_SINGLE: "${attr:equals('test\\x')}", // Should handle unknown escape as literal
    STRING_ENHANCED_ESCAPE_DOUBLE: '${attr:equals("test\\z")}', // Should handle unknown escape as literal
    STRING_NO_NEWLINE_SINGLE: "${attr:equals('no", // Should not allow newlines in string literals
    STRING_NO_NEWLINE_DOUBLE: '${attr:equals("no', // Should not allow newlines in string literals
    STRING_NO_TAB_SINGLE: "${attr:equals('no	test')}", // Should not allow literal tabs
    STRING_NO_TAB_DOUBLE: '${attr:equals("no	test")}', // Should not allow literal tabs

    // Comment support tests (new grammar feature)
    COMMENT_SIMPLE: '# This is a comment\n${attr}', // Should parse comment and expression
    COMMENT_NO_PARAMETER: '# This is not a parameter reference\n${value}', // Should not treat as parameter
    COMMENT_MIXED_CONTENT: 'Hello # This is a comment\n${name} World', // Should handle comments in mixed content
    COMMENT_AFTER_EXPRESSION: '${attr} # This is a comment\nNext line', // Should handle comments after expressions
    COMMENT_MULTILINE: '# Comment 1\n# Comment 2\n${attr}', // Should handle multiple comments
    COMMENT_EMPTY: '#\n${attr}', // Should handle empty comments

    // Semicolon support tests (new grammar feature)
    SEMICOLON_IN_FUNCTION: '${attr:equals("test;value")}', // Should handle semicolons in strings
    SEMICOLON_AS_SEPARATOR: '${attr}; ${other}', // Should handle semicolons as separators in text
    SEMICOLON_IN_MIXED_CONTENT: 'First: ${value1}; Second: ${value2}' // Should handle semicolons in mixed content
};
