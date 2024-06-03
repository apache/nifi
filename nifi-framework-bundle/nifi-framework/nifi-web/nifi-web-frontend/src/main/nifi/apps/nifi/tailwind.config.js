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

const { createGlobPatternsForDependencies } = require('@nx/angular/tailwind');
const { join } = require('path');

/** @type {import('tailwindcss').Config} */
module.exports = {
    content: [join(__dirname, 'src/**/!(*.stories|*.spec).{ts,html}'), ...createGlobPatternsForDependencies(__dirname)],
    theme: {
        extend: {
            fontFamily: {
                roboto: ['Roboto']
            },
            fontSize: {
                xs: [
                    '10px',
                    {
                        lineHeight: '14px',
                        letterSpacing: '0.4px',
                        fontWeight: '400'
                    }
                ],
                sm: [
                    '12px',
                    {
                        lineHeight: '16px',
                        letterSpacing: '0.4px',
                        fontWeight: '400'
                    }
                ],
                base: [
                    '14px',
                    {
                        lineHeight: '20px',
                        letterSpacing: 'normal',
                        fontWeight: '400'
                    }
                ],
                lg: [
                    '16px',
                    {
                        lineHeight: '24px',
                        letterSpacing: 'normal',
                        fontWeight: '400'
                    }
                ],
                xl: [
                    '18px',
                    {
                        lineHeight: '28px',
                        letterSpacing: 'normal',
                        fontWeight: '400'
                    }
                ],
                '2xl': [
                    '20px',
                    {
                        lineHeight: '28px',
                        letterSpacing: 'normal',
                        fontWeight: '400'
                    }
                ],
                '3xl': [
                    '32px',
                    {
                        lineHeight: '40px',
                        letterSpacing: 'normal',
                        fontWeight: '400'
                    }
                ]
            }
        }
    },
    plugins: []
};
