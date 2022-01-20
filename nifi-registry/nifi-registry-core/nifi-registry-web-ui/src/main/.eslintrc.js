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

const OFF = 0; // turn the rule off
const WARNING = 1; // turn the rule on as a warning (doesn’t affect exit code)
const ERROR = 2; // turn the rule on as an error (exit code is 1 when triggered)

module.exports = {
    'root': true,
    'extends': 'eslint-config-airbnb-base',
    'env': {
        'browser': true,
        'es6': true,
        'jasmine': true
    },
    'parserOptions': {
        'ecmaVersion': 2017,
        'sourceType': 'module'
    },
    'parser': '@typescript-eslint/parser',
    'plugins': [
        'jasmine',
        '@typescript-eslint'
    ],
    overrides: [
        {
            // Legacy Javascript files
            files: ['*.js'],
            rules: {
                'dot-notation': OFF,
                'prefer-arrow-callback': OFF,
                'no-var': OFF,
                'no-redeclare': OFF,
                'no-shadow': OFF,
                'quote-props': OFF,
                'object-shorthand': OFF,
                'vars-on-top': OFF,
                'no-param-reassign': OFF,
                'block-scoped-var': OFF,
                'prefer-destructuring': OFF,
                'prefer-template': OFF,
                'consistent-return': OFF,
                'no-restricted-properties': OFF,
                'no-use-before-define': OFF,
                'object-curly-spacing': OFF,
                'newline-per-chained-call': OFF,
                'no-bitwise': OFF,
                'no-nested-ternary': OFF,
                'no-useless-escape': OFF,
                'no-prototype-builtins': OFF,
                'arrow-body-style': OFF,
                'no-else-return': OFF,
            }
        },
        {
            // Typescript files
            files: ['*.ts'],
            rules: {
                'dot-notation': OFF,
                'no-shadow': OFF,
                'no-use-before-define': OFF,
                '@typescript-eslint/no-unused-vars': [ERROR, { args: 'none' }]
            }
        },
        {
            // Test files
            files: ['*.spec.ts'],
            rules: {
                'max-classes-per-file': OFF
            }
        }
    ],
    'rules': {
        // Customize for all files
        'no-unused-vars': [ERROR, { 'vars': 'all' }],
        'lines-between-class-members': [ERROR, 'always', { 'exceptAfterSingleLine': true }],
        'indent': [ERROR, 4, { 'SwitchCase': 1 }],
        'jasmine/no-focused-tests': ERROR,
        'class-methods-use-this': OFF,
        'prefer-destructuring': [ERROR, {
            'VariableDeclarator': {
                'array': false,
                'object': true
            },
            'AssignmentExpression': {
                'array': false,
                'object': false
            }
        }],

        // Disable for all files
        'max-len': OFF,
        'func-names': OFF,
        'spaced-comment': OFF,
        'comma-dangle': OFF,
        'import/extensions': OFF,
        'import/no-unresolved': OFF,
        'import/no-extraneous-dependencies': OFF,
        'no-plusplus': OFF,
        'react/no-this-in-sfc': OFF,
        'prefer-promise-reject-errors': OFF,
        'object-curly-newline': OFF,
        'no-restricted-globals': OFF,
        'import/prefer-default-export': OFF,
        'linebreak-style': OFF,
        'quotes': [ERROR, 'single', { 'allowTemplateLiterals': true }],
        'no-useless-constructor': OFF,
        'no-empty-function': OFF,
        'no-underscore-dangle': ['error', { 'allowAfterThis': true }]
    },
};
