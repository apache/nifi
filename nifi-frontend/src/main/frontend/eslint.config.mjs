import { FlatCompat } from '@eslint/eslintrc';
import { dirname } from 'path';
import { fileURLToPath } from 'url';
import js from '@eslint/js';
import globals from 'globals';
import nxEslintPlugin from '@nx/eslint-plugin';

const compat = new FlatCompat({
    baseDirectory: dirname(fileURLToPath(import.meta.url)),
    recommendedConfig: js.configs.recommended
});

export default [
    {
        ignores: ['**/dist', '**/out-tsc']
    },
    { plugins: { '@nx': nxEslintPlugin } },
    {
        files: ['**/*.ts', '**/*.tsx', '**/*.js', '**/*.jsx'],
        rules: {
            '@nx/enforce-module-boundaries': [
                'error',
                {
                    enforceBuildableLibDependency: true,
                    allow: [],
                    depConstraints: [
                        {
                            sourceTag: '*',
                            onlyDependOnLibsWithTags: ['*']
                        }
                    ]
                }
            ]
        }
    },
    ...compat
        .config({
            extends: ['plugin:@nx/typescript']
        })
        .map((config) => ({
            ...config,
            files: ['**/*.ts', '**/*.tsx', '**/*.cts', '**/*.mts'],
            rules: {
                ...config.rules,
                '@typescript-eslint/no-unused-vars': [
                    'warn',
                    {
                        argsIgnorePattern: '^_',
                        varsIgnorePattern: '^_',
                        caughtErrorsIgnorePattern: '^_'
                    }
                ]
            }
        })),
    ...compat
        .config({
            extends: ['plugin:@nx/javascript']
        })
        .map((config) => ({
            ...config,
            files: ['**/*.js', '**/*.jsx', '**/*.cjs', '**/*.mjs'],
            rules: {
                ...config.rules,
                '@typescript-eslint/no-unused-vars': 'off',
                'no-unused-vars': [
                    'warn',
                    {
                        argsIgnorePattern: '^_',
                        varsIgnorePattern: '^_',
                        caughtErrorsIgnorePattern: '^_'
                    }
                ]
            }
        })),
    {
        files: ['**/*.spec.ts', '**/*.spec.tsx', '**/*.spec.js', '**/*.spec.jsx'],
        languageOptions: {
            globals: {
                ...globals.jest
            }
        }
    }
];
