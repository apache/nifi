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

import { parser } from './nfel';
import type { SyntaxNode } from '@lezer/common';

function findFirst(node: SyntaxNode, name: string): SyntaxNode | null {
    const cur: SyntaxNode | null = node;
    // Depth-first search
    for (let ch = cur.firstChild; ch; ch = ch.nextSibling) {
        if (ch.type.name === name) return ch;
        const nested = findFirst(ch, name);
        if (nested) return nested;
    }
    return null;
}

function collectNames(node: SyntaxNode): string[] {
    const names: string[] = [];
    const stack: SyntaxNode[] = [node];
    while (stack.length) {
        const n = stack.pop()!;
        names.push(n.type.name);
        for (let ch = n.firstChild; ch; ch = ch.nextSibling) stack.push(ch);
    }
    return names;
}

describe('NFEL grammar classification', () => {
    it('parses standalone function after ${ as StandaloneFunction (not AttributeRef)', () => {
        const doc = '${uuid()}';
        const tree = parser.parse(doc);
        const root = tree.topNode;

        const ref = findFirst(root, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();

        const standalone = ref && findFirst(ref, 'StandaloneFunction');
        const attributeRef = ref && findFirst(ref, 'AttributeRef');
        expect(standalone).toBeTruthy();
        expect(attributeRef).toBeNull();

        const fnName = standalone && findFirst(standalone, 'standaloneFunctionName');
        expect(fnName).toBeTruthy();
    });

    it('parses chained function name as FunctionCall following attribute', () => {
        const doc = '${attr:toUpper()}';
        const tree = parser.parse(doc);
        const root = tree.topNode;

        const ref = findFirst(root, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();

        const attr = ref && findFirst(ref, 'AttributeRef');
        expect(attr).toBeTruthy();

        const call = ref && findFirst(ref, 'FunctionCall');
        expect(call).toBeTruthy();
        const fnName = call && findFirst(call, 'functionName');
        expect(fnName).toBeTruthy();
    });

    it('parses nested standalone function then chained function', () => {
        const doc = '${uuid():substring(0,2)}';
        const tree = parser.parse(doc);
        const ref = findFirst(tree.topNode, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();

        // First term is standalone, then a FunctionCall
        const standalone = ref && findFirst(ref, 'StandaloneFunction');
        expect(standalone).toBeTruthy();
        const call = ref && findFirst(ref, 'FunctionCall');
        expect(call).toBeTruthy();
    });

    it('parses parameter reference inside expression', () => {
        const doc = '${#{param}:toUpper()}';
        const tree = parser.parse(doc);
        const ref = findFirst(tree.topNode, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();
        const param = ref && findFirst(ref, 'ParameterReference');
        expect(param).toBeTruthy();
        const paramName = param && findFirst(param, 'ParameterName');
        expect(paramName).toBeTruthy();
    });

    it('includes expression delimiters in ReferenceOrFunction', () => {
        const doc = '${attr}';
        const tree = parser.parse(doc);
        const ref = findFirst(tree.topNode, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();

        // ExpressionOpen is the combined "${"
        const open = ref && findFirst(ref, 'ExpressionStart');
        expect(open).toBeTruthy();

        // Closing brace token '}' should be within the same ReferenceOrFunction
        const names = ref ? collectNames(ref) : [];
        expect(names).toContain('}');
    });
});

describe('NFEL grammar hardening', () => {
    it('distinguishes attribute vs standalone function based on parentheses', () => {
        const attrDoc = '${length}';
        const fnDoc = '${length()}';

        const attrTree = parser.parse(attrDoc);
        const fnTree = parser.parse(fnDoc);

        const attrRef = findFirst(attrTree.topNode, 'ReferenceOrFunction');
        const attrNode = attrRef && findFirst(attrRef, 'AttributeRef');
        const standaloneInAttr = attrRef && findFirst(attrRef, 'StandaloneFunction');
        expect(attrNode).toBeTruthy();
        expect(standaloneInAttr).toBeNull();

        const fnRef = findFirst(fnTree.topNode, 'ReferenceOrFunction');
        const standalone = fnRef && findFirst(fnRef, 'StandaloneFunction');
        const attrInFn = fnRef && findFirst(fnRef, 'AttributeRef');
        expect(standalone).toBeTruthy();
        expect(attrInFn).toBeNull();
    });

    it('handles escaped dollars before expressions', () => {
        const doc = '$$${attr}';
        const tree = parser.parse(doc);
        const root = tree.topNode;

        // Should contain EscapedDollar and a ReferenceOrFunction
        const ref = findFirst(root, 'ReferenceOrFunction');
        expect(ref).toBeTruthy();

        // Collect node type names to assert presence of EscapedDollar
        const names = collectNames(root);
        expect(names).toContain('EscapedDollar');
    });

    it('treats stray closing brace outside expressions as Text', () => {
        const doc = 'plain } here';
        const tree = parser.parse(doc);
        const names = collectNames(tree.topNode);
        // No EL or parameter nodes should be present
        expect(names.includes('ReferenceOrFunction')).toBe(false);
        expect(names.includes('ParameterReference')).toBe(false);
    });

    it('parses comment lines and does not consume parameter references', () => {
        // Comment (not followed by {) then expression
        const doc1 = '# A comment line\n${attr}';
        const tree1 = parser.parse(doc1);
        expect(findFirst(tree1.topNode, 'Comment')).toBeTruthy();
        expect(findFirst(tree1.topNode, 'ReferenceOrFunction')).toBeTruthy();

        // Parameter reference should not be a Comment
        const doc2 = '#{param}';
        const tree2 = parser.parse(doc2);
        expect(findFirst(tree2.topNode, 'ParameterReference')).toBeTruthy();
        expect(findFirst(tree2.topNode, 'Comment')).toBeNull();
    });

    it('recognizes multi-attribute functions as MultiAttrFunction', () => {
        const samples = [
            '${anyAttribute("x")}',
            '${allAttributes("x")}',
            '${anyMatchingAttribute("x")}',
            '${allMatchingAttributes("x")}'
        ];
        samples.forEach((doc) => {
            const tree = parser.parse(doc);
            const multi = findFirst(tree.topNode, 'MultiAttrFunction');
            expect(multi).toBeTruthy();
        });
    });

    it('parses boolean, number, and null literals in arguments', () => {
        const docs = ['${attr:equals(true)}', '${size:gt(42)}', '${value:plus(3.14)}', '${attr:equals(null)}'];
        const expectNodes = ['BooleanLiteral', 'WholeNumber', 'Decimal', 'Null'];
        docs.forEach((doc, idx) => {
            const tree = parser.parse(doc);
            const names = collectNames(tree.topNode);
            expect(names).toContain(expectNodes[idx]);
        });
    });

    it('parses quoted and unquoted parameter names', () => {
        const docs = ['#{param}', '#{"param with spaces"}', '#{param with spaces}'];
        docs.forEach((doc) => {
            const tree = parser.parse(doc);
            const param = findFirst(tree.topNode, 'ParameterReference');
            expect(param).toBeTruthy();
            const name = param && findFirst(param, 'ParameterName');
            expect(name).toBeTruthy();
        });
    });
});
