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

module.exports = {
    "extends": "eslint-config-airbnb",
    "env": {
        "browser": true,
        "es6": true,
        "jasmine": true,
        "jquery": true
    },
    "parserOptions": {
        "ecmaVersion": 2017,
        "sourceType": "module"
    },
    "parser": "@typescript-eslint/parser",
    "plugins": ["@typescript-eslint"],
    overrides: [
        {
            // Legacy Javascript files
            files: ['*.js'],
            rules: {
                "dot-notation": 0,
                "prefer-arrow-callback": 0,
                "no-var": 0,
                "no-redeclare": 0,
                "no-shadow": 0,
                "quote-props": 0,
                "object-shorthand": 0,
                "vars-on-top": 0,
                "no-param-reassign": 0,
                "block-scoped-var": 0,
                "prefer-destructuring": 0,
                "prefer-template": 0,
                "consistent-return": 0,
                "no-restricted-properties": 0,
                "no-use-before-define": 0,
                "object-curly-spacing": 0,
                "newline-per-chained-call": 0,
                "no-bitwise": 0,
                "no-nested-ternary": 0,
                "no-useless-escape": 0,
                "no-prototype-builtins": 0,
            }
        },
        {
            // Typescript files
            files: ['*.ts'],
            rules: {
                '@typescript-eslint/no-unused-vars': [2, { args: "none" }]
            }
        }
    ],
    "rules": {
        // Customize for all files
        "no-unused-vars": ["error", { "args": "none" }],
        "lines-between-class-members": ["error", "always", { "exceptAfterSingleLine": true }],
        "indent": ["error", 4],

        // Disable for all files
        "max-len": 0,
        "func-names": 0,
        "spaced-comment": 0,
        "comma-dangle": 0,
        "import/extensions": 0,
        "import/no-unresolved": 0,
        "import/no-extraneous-dependencies": 0,
        "no-plusplus": 0,
        "react/no-this-in-sfc": 0,
        "prefer-promise-reject-errors": 0,
        "object-curly-newline": 0,
        "no-restricted-globals": 0,
        "import/prefer-default-export": 0
    }
};
