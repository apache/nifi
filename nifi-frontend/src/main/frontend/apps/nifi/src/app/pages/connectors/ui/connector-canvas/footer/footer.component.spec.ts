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

import { Component, input } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { provideRouter } from '@angular/router';
import { ConnectorCanvasFooterComponent } from './footer.component';
import { BreadcrumbEntity } from '../../../../../state/shared';
import * as ConnectorCanvasSelectors from '../../../state/connector-canvas/connector-canvas.selectors';

@Component({
    standalone: true,
    imports: [ConnectorCanvasFooterComponent],
    template: `<connector-canvas-footer [connectorId]="connectorId()"></connector-canvas-footer>`
})
class TestHostComponent {
    connectorId = input('connector-123');
}

interface SetupOptions {
    connectorId?: string;
    breadcrumb?: BreadcrumbEntity | null;
    currentProcessGroupId?: string | null;
}

function createMockBreadcrumb(overrides: Partial<BreadcrumbEntity> = {}): BreadcrumbEntity {
    return {
        id: 'pg-123',
        permissions: { canRead: true, canWrite: true },
        versionedFlowState: '',
        breadcrumb: {
            id: 'pg-123',
            name: 'Test Process Group'
        },
        ...overrides
    };
}

async function setup(options: SetupOptions = {}) {
    const { connectorId = 'connector-123', breadcrumb = null, currentProcessGroupId = 'pg-123' } = options;

    await TestBed.configureTestingModule({
        imports: [TestHostComponent],
        providers: [
            provideRouter([]),
            provideMockStore({
                selectors: [
                    { selector: ConnectorCanvasSelectors.selectBreadcrumbs, value: breadcrumb },
                    { selector: ConnectorCanvasSelectors.selectProcessGroupId, value: currentProcessGroupId }
                ]
            })
        ]
    }).compileComponents();

    const store = TestBed.inject(MockStore);
    const hostFixture = TestBed.createComponent(TestHostComponent);
    hostFixture.componentRef.setInput('connectorId', connectorId);
    hostFixture.detectChanges();

    const footerDebugEl = hostFixture.debugElement.children[0];
    const component = footerDebugEl.componentInstance as ConnectorCanvasFooterComponent;

    return { fixture: hostFixture, component, store };
}

describe('ConnectorCanvasFooterComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { component } = await setup();
            expect(component).toBeTruthy();
        });

        it('should have connectorId input', async () => {
            const { component } = await setup({ connectorId: 'test-connector' });
            expect(component.connectorId()).toBe('test-connector');
        });
    });

    describe('Route generation', () => {
        it('should generate correct route for connector canvas breadcrumbs', async () => {
            const { component } = await setup({ connectorId: 'my-connector' });
            const routeGenerator = component.routeGenerator();

            const route = routeGenerator('process-group-abc');

            expect(route).toEqual(['/connectors', 'my-connector', 'canvas', 'process-group-abc']);
        });

        it('should use current connectorId in route generation', async () => {
            const { fixture } = await setup({ connectorId: 'initial-connector' });

            fixture.componentRef.setInput('connectorId', 'updated-connector');
            fixture.detectChanges();

            const footerDebugEl = fixture.debugElement.children[0];
            const component = footerDebugEl.componentInstance as ConnectorCanvasFooterComponent;
            const routeGenerator = component.routeGenerator();
            const route = routeGenerator('pg-456');

            expect(route).toEqual(['/connectors', 'updated-connector', 'canvas', 'pg-456']);
        });
    });

    describe('Breadcrumb display', () => {
        it('should render breadcrumbs container', async () => {
            const { fixture } = await setup({
                breadcrumb: createMockBreadcrumb()
            });

            const breadcrumbContainer = fixture.nativeElement.querySelector('.breadcrumb-container');
            expect(breadcrumbContainer).toBeTruthy();
        });

        it('should render footer element', async () => {
            const { fixture } = await setup();

            const footer = fixture.nativeElement.querySelector('footer');
            expect(footer).toBeTruthy();
        });

        it('should include breadcrumbs component', async () => {
            const { fixture } = await setup({
                breadcrumb: createMockBreadcrumb()
            });

            const breadcrumbs = fixture.nativeElement.querySelector('breadcrumbs');
            expect(breadcrumbs).toBeTruthy();
        });
    });

    describe('Store selectors', () => {
        it('should select breadcrumbs from store', async () => {
            const mockBreadcrumb = createMockBreadcrumb({ id: 'test-pg' });
            const { component } = await setup({ breadcrumb: mockBreadcrumb });

            let breadcrumbs: BreadcrumbEntity | null = null;
            component.breadcrumbs$.subscribe((b) => (breadcrumbs = b));

            expect(breadcrumbs).toEqual(mockBreadcrumb);
        });

        it('should select currentProcessGroupId from store', async () => {
            const { component } = await setup({ currentProcessGroupId: 'current-pg-id' });

            let processGroupId: string | null = null;
            component.currentProcessGroupId$.subscribe((id) => (processGroupId = id));

            expect(processGroupId).toBe('current-pg-id');
        });
    });
});
