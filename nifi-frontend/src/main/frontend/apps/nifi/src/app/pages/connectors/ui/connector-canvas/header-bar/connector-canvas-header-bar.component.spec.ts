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
import { Component } from '@angular/core';
import { ConnectorCanvasHeaderBarComponent } from './connector-canvas-header-bar.component';
import { ConnectorService } from '../../../service/connector.service';
import { HttpClient } from '@angular/common/http';
import { ComponentType } from '@nifi/shared';

@Component({
    standalone: true,
    imports: [ConnectorCanvasHeaderBarComponent],
    template: `
        <connector-canvas-header-bar
            [connectorId]="connectorId"
            [selectedComponentId]="selectedComponentId"
            [graphControlsOpen]="graphControlsOpen"
            (backToConnectors)="onBack()"
            (goToComponent)="onGoTo($event)"
            (toggleGraphControls)="onToggleGraphControls()">
        </connector-canvas-header-bar>
    `
})
class TestHostComponent {
    connectorId = 'test-connector-id';
    selectedComponentId: string | null = null;
    graphControlsOpen = true;
    onBack = vi.fn();
    onGoTo = vi.fn();
    onToggleGraphControls = vi.fn();
}

interface SetupOptions {
    connectorId?: string;
    selectedComponentId?: string | null;
    graphControlsOpen?: boolean;
}

async function setup(options: SetupOptions = {}) {
    const mockConnectorService = {
        searchConnector: vi.fn()
    };

    await TestBed.configureTestingModule({
        imports: [TestHostComponent],
        providers: [
            { provide: ConnectorService, useValue: mockConnectorService },
            { provide: HttpClient, useValue: {} }
        ]
    }).compileComponents();

    const hostFixture = TestBed.createComponent(TestHostComponent);
    const host = hostFixture.componentInstance;

    if (options.connectorId !== undefined) {
        host.connectorId = options.connectorId;
    }
    if (options.selectedComponentId !== undefined) {
        host.selectedComponentId = options.selectedComponentId;
    }
    if (options.graphControlsOpen !== undefined) {
        host.graphControlsOpen = options.graphControlsOpen;
    }

    hostFixture.detectChanges();

    return { hostFixture, host, mockConnectorService };
}

describe('ConnectorCanvasHeaderBarComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', async () => {
            const { hostFixture } = await setup();
            const headerBar = hostFixture.nativeElement.querySelector('.connector-header-bar');
            expect(headerBar).toBeTruthy();
        });

        it('should render the search component', async () => {
            const { hostFixture } = await setup();
            const search = hostFixture.nativeElement.querySelector('canvas-header-search');
            expect(search).toBeTruthy();
        });
    });

    describe('Back link', () => {
        it('should render the back link with data-qa attribute', async () => {
            const { hostFixture } = await setup();
            const backLink = hostFixture.nativeElement.querySelector('[data-qa="back-to-connectors"]');
            expect(backLink).toBeTruthy();
        });

        it('should display "Installed Connectors" text', async () => {
            const { hostFixture } = await setup();
            const backLink = hostFixture.nativeElement.querySelector('[data-qa="back-to-connectors"]');
            expect(backLink.textContent).toContain('Installed Connectors');
        });

        it('should render an arrow-left icon', async () => {
            const { hostFixture } = await setup();
            const icon = hostFixture.nativeElement.querySelector('[data-qa="back-to-connectors"] .fa-arrow-left');
            expect(icon).toBeTruthy();
        });

        it('should call host onBack when the link is clicked', async () => {
            const { hostFixture, host } = await setup();
            const backLink = hostFixture.nativeElement.querySelector('[data-qa="back-to-connectors"]');
            backLink.click();
            expect(host.onBack).toHaveBeenCalledTimes(1);
        });
    });

    describe('goToComponent output', () => {
        it('should map onGoToComponent using parentGroup.id when available', async () => {
            const { hostFixture, host } = await setup();

            const headerBarEl = hostFixture.debugElement.children[0];
            const headerBarComponent = headerBarEl.componentInstance as ConnectorCanvasHeaderBarComponent;

            const result = {
                id: 'p1',
                groupId: 'fallback-pg',
                parentGroup: { id: 'parent-pg', name: 'Parent' },
                versionedGroup: { id: '', name: '' },
                name: 'MyProcessor',
                matches: []
            };
            headerBarComponent.onGoToComponent({ result, type: ComponentType.Processor });

            expect(host.onGoTo).toHaveBeenCalledWith({
                id: 'p1',
                type: ComponentType.Processor,
                groupId: 'parent-pg'
            });
        });

        it('should fall back to groupId when parentGroup is null', async () => {
            const { hostFixture, host } = await setup();

            const headerBarEl = hostFixture.debugElement.children[0];
            const headerBarComponent = headerBarEl.componentInstance as ConnectorCanvasHeaderBarComponent;

            const result = {
                id: 'p2',
                groupId: 'fallback-pg',
                parentGroup: null as any,
                versionedGroup: { id: '', name: '' },
                name: 'AnotherProcessor',
                matches: []
            };
            headerBarComponent.onGoToComponent({ result, type: ComponentType.Processor });

            expect(host.onGoTo).toHaveBeenCalledWith({
                id: 'p2',
                type: ComponentType.Processor,
                groupId: 'fallback-pg'
            });
        });
    });

    describe('Graph controls toggle', () => {
        it('should render the toggle button', async () => {
            const { hostFixture } = await setup();
            const toggleBtn = hostFixture.nativeElement.querySelector('[data-qa="toggle-graph-controls"]');
            expect(toggleBtn).toBeTruthy();
        });

        it('should show chevron-left icon when controls are open', async () => {
            const { hostFixture } = await setup({ graphControlsOpen: true });
            const icon = hostFixture.nativeElement.querySelector('[data-qa="toggle-graph-controls"] .fa-chevron-left');
            expect(icon).toBeTruthy();
        });

        it('should show chevron-right icon when controls are closed', async () => {
            const { hostFixture } = await setup({ graphControlsOpen: false });
            const icon = hostFixture.nativeElement.querySelector('[data-qa="toggle-graph-controls"] .fa-chevron-right');
            expect(icon).toBeTruthy();
        });

        it('should call host onToggleGraphControls when toggle button is clicked', async () => {
            const { hostFixture, host } = await setup();
            const toggleBtn = hostFixture.nativeElement.querySelector('[data-qa="toggle-graph-controls"]');
            toggleBtn.click();
            expect(host.onToggleGraphControls).toHaveBeenCalledTimes(1);
        });
    });
});
