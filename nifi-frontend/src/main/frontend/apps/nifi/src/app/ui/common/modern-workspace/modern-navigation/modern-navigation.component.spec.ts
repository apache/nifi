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

import { TestBed } from '@angular/core/testing';
import { provideRouter } from '@angular/router';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { ModernNavigationComponent } from './modern-navigation.component';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { initialState } from '../../../../state/current-user/current-user.reducer';
import { selectFlowConfiguration } from '../../../../state/flow-configuration/flow-configuration.selectors';
import { selectClusterSummary } from '../../../../state/cluster-summary/cluster-summary.selectors';

describe('Modern workspace navigation', () => {
    let store: MockStore;
    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [ModernNavigationComponent],
            providers: [
                provideRouter([]),
                provideMockStore({
                    selectors: [
                        { selector: selectCurrentUser, value: initialState.user },
                        { selector: selectClusterSummary, value: null },
                        { selector: selectFlowConfiguration, value: null }
                    ]
                })
            ]
        });
        store = TestBed.inject(MockStore);
    });
    afterEach(() => store.resetSelectors());
    it('hides privileged destinations and unavailable cluster navigation', () => {
        const fixture = TestBed.createComponent(ModernNavigationComponent);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a[href="/settings"]')).toBeNull();
        expect(fixture.nativeElement.querySelector('a[href="/provenance"]')).toBeNull();
        expect(fixture.nativeElement.querySelector('a[href="/cluster"]')).toBeNull();
        expect(fixture.nativeElement.textContent).toContain('Connectivity unavailable');
    });
    it('updates managed-user and policy shortcuts when permissions are changed', () => {
        store.overrideSelector(selectFlowConfiguration, {
            supportsManagedAuthorizer: true,
            supportsConfigurableAuthorizer: true,
            supportsConfigurableUsersAndGroups: true,
            currentTime: '',
            timeOffset: 0,
            defaultBackPressureObjectThreshold: 10000,
            defaultBackPressureDataSizeThreshold: '1 GB'
        });
        const user = {
            ...initialState.user,
            tenantsPermissions: { canRead: true, canWrite: false },
            policiesPermissions: { canRead: true, canWrite: false }
        };
        const permissions = store.overrideSelector(selectCurrentUser, user);
        const fixture = TestBed.createComponent(ModernNavigationComponent);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a[href="/users"]')).not.toBeNull();
        expect(fixture.nativeElement.querySelector('a[href="/access-policies"]')).toBeNull();
        permissions.setResult({ ...user, policiesPermissions: { canRead: true, canWrite: true } });
        store.refreshState();
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a[href="/access-policies"]')).not.toBeNull();
        permissions.setResult(initialState.user);
        store.refreshState();
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a[href="/users"]')).toBeNull();
        expect(fixture.nativeElement.querySelector('a[href="/access-policies"]')).toBeNull();
    });

    it('exposes authorized routes and keeps overview/canvas navigation local', () => {
        store.overrideSelector(selectCurrentUser, {
            ...initialState.user,
            controllerPermissions: { canRead: true, canWrite: false },
            provenancePermissions: { canRead: true, canWrite: false }
        });
        const fixture = TestBed.createComponent(ModernNavigationComponent);
        fixture.componentRef.setInput('designer', true);
        const overview = vi.fn();
        const canvas = vi.fn();
        fixture.componentInstance.overviewRequested.subscribe(overview);
        fixture.componentInstance.canvasRequested.subscribe(canvas);
        fixture.detectChanges();
        expect(fixture.nativeElement.querySelector('a[href="/settings"]')).not.toBeNull();
        expect(fixture.nativeElement.querySelector('a[href="/provenance"]')).not.toBeNull();
        fixture.nativeElement.querySelector('button[title="Show current group summary below the canvas"]').click();
        fixture.nativeElement.querySelector('button[title="Designer"]').click();
        expect(overview).toHaveBeenCalledOnce();
        expect(canvas).toHaveBeenCalledOnce();
    });
});
