import { FlatCompat } from '@eslint/eslintrc';
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import js from '@eslint/js';
import baseConfig from '../../eslint.config.mjs';

const compat = new FlatCompat({
    baseDirectory: dirname(fileURLToPath(import.meta.url)),
    recommendedConfig: js.configs.recommended
});

export default [
    {
        ignores: ['**/dist', '**/out-tsc']
    },
    ...baseConfig,
    {
        languageOptions: {
            parserOptions: {
                ecmaVersion: 'latest'
            }
        }
    },
    ...compat
        .config({
            extends: [
                'plugin:@nx/angular',
                'eslint:recommended',
                'plugin:@typescript-eslint/recommended',
                'plugin:@angular-eslint/recommended',
                'plugin:@angular-eslint/template/process-inline-templates',
                'plugin:prettier/recommended'
            ]
        })
        .map((config) => ({
            ...config,
            files: ['**/*.ts'],
            rules: {
                ...config.rules,
                '@angular-eslint/directive-selector': [
                    'error',
                    {
                        type: 'attribute',
                        prefix: '',
                        style: 'camelCase'
                    }
                ],
                '@angular-eslint/component-selector': [
                    'error',
                    {
                        type: 'element',
                        prefix: '',
                        style: 'kebab-case'
                    }
                ],
                '@angular-eslint/component-class-suffix': 'off',
                '@typescript-eslint/no-explicit-any': 'off',
                '@typescript-eslint/ban-ts-comment': 'off',
                '@typescript-eslint/no-non-null-assertion': 'off',
                '@typescript-eslint/no-unused-vars': [
                    'warn',
                    {
                        argsIgnorePattern: '^_',
                        varsIgnorePattern: '^_',
                        caughtErrorsIgnorePattern: '^_'
                    }
                ],
                'no-useless-escape': 'off',
                '@angular-eslint/prefer-standalone': 'off'
            }
        })),
    ...compat
        .config({
            extends: ['plugin:@nx/angular-template']
        })
        .map((config) => ({
            ...config,
            files: ['**/*.html'],
            rules: {
                ...config.rules
            }
        }))
];
