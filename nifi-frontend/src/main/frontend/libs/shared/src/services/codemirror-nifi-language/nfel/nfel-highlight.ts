import { styleTags, tags as t } from '@lezer/highlight';

export const nfelHighlight = styleTags({
    // Expression delimiters - the ${ and } parts
    ExpressionStart: t.special(t.brace),
    ParameterStart: t.special(t.brace),
    '{ }': t.brace,
    '( )': t.paren,
    ':': t.operator,
    ',': t.separator,

    // Text and escaping
    Text: t.content,
    EscapedDollar: t.escape,

    // Comments
    Comment: t.comment,

    // Function names - target the actual nodes that exist in the grammar
    FunctionCall: t.function(t.variableName),
    StandaloneFunction: t.function(t.variableName),

    // Multi-attribute function names
    MultiAttrFunctionName: t.function(t.variableName),

    // Attribute names - target the actual nodes that exist in the grammar
    AttrName: t.variableName,
    SingleAttrName: t.variableName,

    // Parameter references - structure parsing works well, name tokenization is a minor TODO
    ParameterReference: t.special(t.variableName),
    ParameterName: t.variableName, // For when ParameterName tokens are properly recognized
    'parameterName identifier': t.variableName, // Fallback for parameter names

    // Fallback for untagged identifiers - make them stand out as variables
    identifier: t.variableName,

    // Handle error nodes within expressions as identifiers (for broken parsing)
    '⚠': t.variableName,

    // Additional structural nodes that should have basic highlighting
    ReferenceOrFunction: t.content,
    AttributeRefOrFunctionCall: t.content,
    AttributeRef: t.content,
    Subject: t.content,
    SingleAttrRef: t.content,

    // Literals
    StringLiteral: t.string,
    WholeNumber: t.number,
    Decimal: t.number,
    BooleanLiteral: t.bool,
    Null: t.null,

    // Structural elements
    'ReferenceOrFunction Expression': t.content,
    'AttributeRefOrFunctionCall FunctionCall StandaloneFunction': t.content,
    'AttributeRef Subject AttrName': t.content,
    'SingleAttrName MultiAttrName': t.content,
    'ParameterReference ParameterContent': t.special(t.variableName)
});
