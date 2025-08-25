# NIFI Expression Language (NFEL) Testing Guide

This directory contains the NFEL grammar, parser, and comprehensive test patterns for CodeMirror v6 syntax highlighting.

## 🎯 Quick Manual Testing

Copy and paste the following comprehensive test string into a CodeMirror editor in the NiFi UI to manually verify all syntax highlighting cases:

```
# ===== NIFI Expression Language (NFEL) Comprehensive Test Cases =====
# Copy this entire block into a CodeMirror editor to test all syntax highlighting

# Basic Expressions
${attr}
${uuid()}
#{param}
${attr:toUpper()}

# String Literals (should be var(--editor-string))
${attr:equals("test")}
${attr:contains('value')}

# Complex Nesting
${filename:substring(0, ${name:length()})}
${filename:replace(${attr:substring(0, 3)}, "new")}

# Parameter References (should be var(--editor-parameter))
#{parameter Name}
#{'A lonnnnggg parameter name.'}
#{parameterName}
#{database.url}
#{app-config}
#{env_var}
#{'param with spaces'}
#{"param with special-chars"}

# Expression Escaping (should be plain text)
Price: $$50 for items
$$${attr} - escaped dollar + expression
$$$${UserName} - double escaped

# Mixed Content with Plain Text
Hello ${name}, you have ${count} items.
Result: ${filename:replace(${attr}, "new")} - Status: ${status}
Price: $$100 for ${item}

# Function Chaining (functions should be var(--editor-el-function))
${attr:equals('value'):contains('test')}
${text:trim():toUpper():substring(0, 5)}

# Embedded Parameters (should be var(--editor-parameter))
${#{param}:toUpper()}
${value:ifElse(#{defaultValue}, "empty")}

# Boolean and Numbers (should be var(--editor-number))
${attr:equals(true)}
${size:gt(1000)}
${value:plus(3.14)}

# Complex Real-World Examples
${filename:substring(0, ${filename:lastIndexOf(".")}):toUpper():prepend(#{prefix}):append("_PROCESSED")}
${json:jsonPath("$.user.email"):replaceNull("noemail@example.com")}

# Edge Cases and Special Patterns
${attr:substring(${start}, ${end})} - Multiple nested expressions
${attr:replace(#{search}, ${replacement:toUpper()})} - Mixed parameter and expression nesting
${attr:contains(${other:substring(${start:toNumber()}, 5)})} - Deep nesting with type conversion
${path:replace(${dir:append("/")}${file:substring(0, ${len:toNumber()})}, ".txt")} - Complex path manipulation

# Incomplete Expression Edge Cases (should parse gracefully)
${attr:toUpper():
${attr:trim():toUpper():
#{"
#{'myParam
${attr:toUpper()} more text

# Enhanced Grammar Features
# This is a comment (should be var(--editor-comment))
${attr:equals("test;value")} # comment after expression
${attr}; ${other} - semicolon separator

# String Escaping
${attr:equals('He said \'hello\'')}
${path:equals("C:\\folder\\file.txt")}
${text:equals("Line 1\nLine 2\tTabbed")}

# Math and Boolean Operations
${value1:plus(${value2:multiply(10)})}
${size:gt("1000"):and(${type:equals("file")})}
${status:equals("success"):or(${retry:lt("3")})}

# JSON Operations
${json:jsonPath("$.user.id")}
${data:jsonPathSet("$.processed", "true")}

# Date and Time
${now():format("yyyy-MM-dd")}
${timestamp:toDate("yyyy-MM-dd HH:mm:ss")}

# File Processing
${filename:substring(${filename:lastIndexOf("."):plus(1)})}
${filename:replaceAll("[^a-zA-Z0-9._-]", "_")}

# Encoding and Hashing
${content:hash("SHA-256")}
${text:urlEncode()}
${encoded:base64Decode()}

# Multi-Attribute Functions
${anyAttribute("user.*")}
${allAttributes("system.*")}

# URI Operations
${getUri("https", "example.com", 443, "/api", "param=value", "user", "pass")}

# Regular Expressions
${text:find("\\d+")}
${content:replaceByPattern("\\s+", " ")}

# State and Thread Operations
${getStateValue("counter")}
${thread()}

# Delineated Values
${anyDelineatedValue(",", 2)}
${allDelineatedValues("|")}

# Data Validation
${email:matches("^[\\w\\.-]+@[\\w\\.-]+\\.[a-zA-Z]{2,}$")}
${phone:matches("^\\+?[1-9]\\d{1,14}$")}

# Text Manipulation
${text:trim():replaceAll("\\s+", " ")}
${text:escapeJson()}
${html:escapeHtml4()}
${csv:escapeCsv()}

# Conditional Logic
${value:isEmpty():ifElse(#{defaultValue}, ${value})}
${text:isNull():not():ifElse(${text:toUpper()}, "")}

# Performance Optimization
${largeText:substring(0, 1000):hash("MD5")}

# Error Cases (should still parse gracefully)
${unclosed
${:missing}
${attr:}
${nested${broken}

# ===== Expected Nfel Grammar Color Coding =====
# Plain Text: Default color (black/white depending on theme)
# ${} Delimiters: var(--editor-bracket)
# Function Names: var(--editor-el-function) (toUpper, equals, contains, etc.)
# Parameter Names: var(--editor-parameter)
# String Literals: var(--editor-string) ("test", 'value', etc.)
# Numbers: var(--editor-number) (100, 3.14, true, false, null)
# Comments: var(--editor-comment) (# This is a comment)
# Escaped Text: Plain text ($$, escaped content)
```

## 🧪 What to Verify

When testing in the CodeMirror editor, verify these color codings:

### ✅ Correct Styling (Light Theme)

- **Plain Text**: Regular text outside expressions should be black (default text color)
- **Expression Delimiters**: `${`, `}` should be var(--editor-bracket)
- **Parameter Delimiters**: `#{`, `}` should be var(--editor-bracket)
- **Matching Brackets**: When cursor is between `{}`, brackets should have:
    - Background: `--nf-neutral`
    - Color: `--mat-sys-inverse-on-surface` (black in light mode)
- **Non-matching Brackets**: When brackets don't match, should have:
    - Color: `--mat-sys-on-surface` (default text color)
- **Functions**: `toUpper`, `equals`, `contains` should be (`--editor-el-function`)
- **Parameters**: should be (`--editor-parameter`)
- **Strings**: `"test"`, `'value'` should be (`--editor-string`)
- **Numbers**: `100`, `3.14`, `true`, `false` should be (`--editor-number`)
- **Comments**: `# This is a comment` should be (`--editor-comment`)

### ✅ Correct Styling (Dark Theme)

- **Plain Text**: Regular text outside expressions should be white (default text color)
- **Expression Delimiters**: `${`, `}` should be var(--editor-bracket)
- **Parameter Delimiters**: `#{`, `}` should be var(--editor-bracket)
- **Matching Brackets**: When cursor is between `{}`, brackets should have:
    - Background: `--nf-neutral`: `#acacac`
    - Color: `--mat-sys-inverse-on-surface` (white in dark mode)
- **Non-matching Brackets**: When brackets don't match, should have:
    - Color: `--mat-sys-on-surface` (default text color)
- **Attributes**: `attr`, `filename`, `name` inside expressions should be (`--editor-keyword`)
- **Functions**: `toUpper`, `equals`, `contains` should be (`--editor-el-function`)
- **Parameters**: should be (`--editor-parameter`)
- **Strings**: `"test"`, `'value'` should be (`--editor-string`)
- **Numbers**: `100`, `3.14`, `true`, `false` should be (`--editor-number`)
- **Comments**: `# This is a comment` should be (`--editor-comment`)

### ✅ Plain Text (No Special Styling)

- **Text outside expressions**: `Hello`, `Price:`, `Result:` should be default color
- **Escaped dollars**: `$$` should be plain text, not styled as links
- **Colons in plain text**: `:` in `Result:` should be plain text

## 📁 Files in this Directory

- **`nfel.grammar`**: Lezer grammar definition for NFEL parsing
- **`nfel.ts`**: Compiled Lezer parser
- **`nfel.terms.ts`**: Grammar term definitions
- **`nfel-example.ts`**: Test patterns used in automated tests (this file's patterns)

## 🔧 Development Notes

- The test patterns in `nfel-example.ts` are used by `codemirror-nifi-language.service.spec.ts`
- Grammar compliance is tested by parsing all patterns in `NFEL_PATTERNS`
- Theme validation tests ensure correct CSS class application
- Any changes to grammar should be tested with the comprehensive test string above

## 🐛 Common Issues to Watch For

1. **Plain text coloring**: Ensure `Result:`, `Price:` don't get attribute colors
2. **Escaped dollars**: `$$` should be plain text, not link-colored
3. **Numbers always get coloring**: `100` in `Price: $$100` should be colored as a number
4. **Boolean literals**: `true`/`false` should be colored as numbers
5. **Bracket/Parenthesis matching**: any matching `{}`, `()` should all have consistent bracket styling

## 🚀 Testing Workflow

1. **Copy the test string above** into a CodeMirror editor in NiFi UI
2. **Verify color coding** matches the expected styling
3. **Test autocompletion** by typing incomplete expressions like `${al` or `#{par`
4. **Check edge cases** like escaped expressions and malformed syntax
5. **Run automated tests** with `npm test -- shared --testPathPattern="codemirror-nifi-language"`
