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

import { Component } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { Title } from '@angular/platform-browser';
import { Router, TitleStrategy, provideRouter } from '@angular/router';
import { RouterTestingHarness } from '@angular/router/testing';
import { PageTitleStrategy } from './page-title.strategy';

@Component({ template: '' })
class TestPage {}

describe('Page title routing', () => {
    beforeEach(() => {
        TestBed.configureTestingModule({
            providers: [
                { provide: TitleStrategy, useClass: PageTitleStrategy },
                provideRouter([
                    {
                        path: 'summary',
                        title: 'Summary',
                        children: [
                            { path: '', pathMatch: 'full', redirectTo: 'processors' },
                            {
                                path: 'processors',
                                title: 'Processors',
                                children: [
                                    { path: '', component: TestPage },
                                    { path: ':id', component: TestPage }
                                ]
                            }
                        ]
                    },
                    { path: 'login', title: 'Sign in', component: TestPage },
                    {
                        path: 'settings',
                        title: 'Controller Settings',
                        loadChildren: () =>
                            Promise.resolve([
                                { path: 'general', component: TestPage },
                                { path: 'providers', title: 'Parameter Providers', component: TestPage }
                            ])
                    },
                    { path: 'blocked', title: 'Users', canActivate: [() => false], component: TestPage },
                    { path: 'untitled', component: TestPage }
                ])
            ]
        });
    });

    it('uses the redirected page title and retains it for component ids and query strings', async () => {
        const harness = await RouterTestingHarness.create();
        await harness.navigateByUrl('/summary');
        expect(TestBed.inject(Title).getTitle()).toBe('Processors - Apache NiFi');
        await harness.navigateByUrl('/summary/processors/private-id?filter=something');
        expect(TestBed.inject(Title).getTitle()).toBe('Processors - Apache NiFi');
        await harness.navigateByUrl('/login');
        expect(TestBed.inject(Title).getTitle()).toBe('Sign in - Apache NiFi');
    });

    it('inherits lazy-route context, uses a child title and resets missing titles', async () => {
        const harness = await RouterTestingHarness.create();
        await harness.navigateByUrl('/settings/general');
        expect(TestBed.inject(Title).getTitle()).toBe('Controller Settings - Apache NiFi');
        await harness.navigateByUrl('/settings/providers');
        expect(TestBed.inject(Title).getTitle()).toBe('Parameter Providers - Apache NiFi');
        await harness.navigateByUrl('/untitled');
        expect(TestBed.inject(Title).getTitle()).toBe('Apache NiFi');
    });

    it('does not adopt the title of a navigation rejected by its guard', async () => {
        await RouterTestingHarness.create('/summary/processors');
        expect(await TestBed.inject(Router).navigateByUrl('/blocked')).toBe(false);
        expect(TestBed.inject(Title).getTitle()).toBe('Processors - Apache NiFi');
    });
});
