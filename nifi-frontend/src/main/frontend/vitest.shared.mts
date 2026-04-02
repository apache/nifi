/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/// <reference types="vitest" />
import { defineConfig } from 'vitest/config';
import angular from '@analogjs/vite-plugin-angular';
import tsconfigPaths from 'vite-tsconfig-paths';

export default defineConfig({
    plugins: [angular(), tsconfigPaths()],
    test: {
        globals: true,
        environment: 'happy-dom',
        environmentOptions: {
            happyDOM: {
                settings: {
                    disableJavaScriptFileLoading: true,
                    disableCSSFileLoading: true,
                    disableJavaScriptEvaluation: true,
                    handleDisabledFileLoadingAsSuccess: true
                }
            }
        },
        include: ['src/**/*.spec.ts'],
        reporters: ['default'],
        pool: 'threads',
        coverage: {
            provider: 'v8',
            reporter: ['cobertura']
        },
        deps: {
            optimizer: {
                ssr: {
                    enabled: true,
                    include: [
                        'rxjs',
                        'rxjs/**',
                        '@ngrx/**',
                        '@angular/**',
                        'd3',
                        'd3-*',
                        'tslib',
                        'zone.js',
                        'zone.js/**',
                        'marked',
                        'ng-mocks',
                        'immer'
                    ]
                }
            }
        }
    }
});
