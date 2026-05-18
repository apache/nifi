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
import { Router } from '@angular/router';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { MockStore, provideMockStore } from '@ngrx/store/testing';

import { ConnectorCanvasRedirector } from './connector-canvas-redirector.component';
import {
    selectConnectorCanvasEntityError,
    selectConnectorCanvasEntityLoadingStatus
} from '../../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { NifiSpinnerDirective } from '@nifi/shared';

interface SetupOptions {
    loadingStatus?: 'pending' | 'loading' | 'success' | 'error';
    error?: string | null;
}

async function setup(options: SetupOptions = {}) {
    const loadingStatus = options.loadingStatus ?? 'loading';
    const error = options.error ?? null;

    const navigateSpy = vi.fn();

    await TestBed.configureTestingModule({
        imports: [ConnectorCanvasRedirector, NoopAnimationsModule, NifiSpinnerDirective],
        providers: [provideMockStore(), { provide: Router, useValue: { navigate: navigateSpy } }]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    store.overrideSelector(selectConnectorCanvasEntityLoadingStatus, loadingStatus);
    store.overrideSelector(selectConnectorCanvasEntityError, error);
    store.refreshState();

    const fixture = TestBed.createComponent(ConnectorCanvasRedirector);
    const component = fixture.componentInstance;
    fixture.detectChanges();

    return { fixture, component, navigateSpy };
}

describe('ConnectorCanvasRedirector', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    it('should create the component', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should show spinner when loading', async () => {
        const { fixture } = await setup({ loadingStatus: 'loading' });

        const spinner = fixture.nativeElement.querySelector('spinner');
        expect(spinner).toBeTruthy();
        expect(fixture.nativeElement.querySelector('[data-qa="connector-resolve-error"]')).toBeNull();
    });

    it('should show error message and return button when error', async () => {
        const { fixture } = await setup({
            loadingStatus: 'error',
            error: 'Could not resolve connector'
        });

        const errorRegion = fixture.nativeElement.querySelector('[data-qa="connector-resolve-error"]');
        expect(errorRegion).toBeTruthy();
        expect(errorRegion.textContent).toContain('Could not resolve connector');

        const returnButton = fixture.nativeElement.querySelector('[data-qa="return-to-connectors"]');
        expect(returnButton).toBeTruthy();

        expect(fixture.nativeElement.querySelector('spinner')).toBeNull();
    });

    it('should call router.navigate on returnToConnectorListing', async () => {
        const { component, navigateSpy } = await setup({
            loadingStatus: 'error',
            error: 'x'
        });

        component.returnToConnectorListing();

        expect(navigateSpy).toHaveBeenCalledWith(['/connectors']);
    });
});
