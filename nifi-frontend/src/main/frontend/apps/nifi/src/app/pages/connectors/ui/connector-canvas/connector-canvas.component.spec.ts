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

import { Component, input, output } from '@angular/core';
import { CommonModule } from '@angular/common';
import { ComponentFixture, TestBed, fakeAsync, tick } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { provideRouter } from '@angular/router';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { firstValueFrom } from 'rxjs';
import { take } from 'rxjs/operators';
import { ComponentType, selectRouteParams, selectUrl } from '@nifi/shared';

import { ConnectorCanvasComponent } from './connector-canvas.component';
import { CanvasComponent } from '../../../../ui/common/canvas/canvas.component';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { setConfiguration } from '../../../../state/canvas-ui/canvas-ui.actions';
import * as ConnectorCanvasSelectors from '../../state/connector-canvas/connector-canvas.selectors';
import { selectParentProcessGroupId } from '../../state/connector-canvas/connector-canvas.selectors';
import {
    deselectAllComponents,
    enterProcessGroup,
    leaveProcessGroup,
    loadConnectorFlow,
    resetConnectorCanvasState,
    selectComponents,
    setSkipTransform
} from '../../state/connector-canvas/connector-canvas.actions';
import { resetConnectorCanvasEntityState } from '../../state/connector-canvas-entity/connector-canvas-entity.actions';

// Mock components to avoid loading complex real components
@Component({
    selector: 'reusable-canvas',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockReusableCanvasComponent {
    labels = input<unknown[]>([]);
    processors = input<unknown[]>([]);
    funnels = input<unknown[]>([]);
    ports = input<unknown[]>([]);
    remoteProcessGroups = input<unknown[]>([]);
    processGroups = input<unknown[]>([]);
    connections = input<unknown[]>([]);
    previewExtensions = input<unknown[]>([]);
    registryClients = input<unknown[]>([]);
    processGroupId = input<string | null>(null);
    selectedComponentIds = input<string[]>([]);
    dataReady = input(false);
    skipInitialCenter = input(false);
    selectComponents = output<Array<{ id: string; type: ComponentType }>>();
    deselectAll = output<void>();
    initialized = output<void>();
    centerOnSelection = vi.fn();
}

@Component({
    selector: 'navigation',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockNavigationComponent {
    heading = input<string>('');
}

@Component({
    selector: 'mock-blocking-dialog',
    standalone: true,
    template: '<p>Dialog</p>'
})
class MockBlockingDialogComponent {}

interface SetupOptions {
    loadingStatus?: 'pending' | 'loading' | 'success' | 'error';
    connectorId?: string | null;
    processGroupId?: string | null;
    url?: string;
    parentProcessGroupId?: string | null;
    skipTransform?: boolean;
    routeParams?: Record<string, string>;
}

const DEFAULT_CONNECTOR_ID = 'connector-1';
const DEFAULT_PROCESS_GROUP_ID = 'pg-root';

function buildMockSelectors(options: SetupOptions = {}) {
    const loadingStatus = options.loadingStatus ?? 'success';
    const connectorId = options.connectorId !== undefined ? options.connectorId : DEFAULT_CONNECTOR_ID;
    const processGroupId = options.processGroupId !== undefined ? options.processGroupId : DEFAULT_PROCESS_GROUP_ID;
    const url = options.url ?? `/connectors/${connectorId}/canvas/${processGroupId}`;
    const parentProcessGroupId = options.parentProcessGroupId !== undefined ? options.parentProcessGroupId : null;
    const skipTransform = options.skipTransform ?? false;
    const routeParamsValue = options.routeParams ?? { id: connectorId, processGroupId };

    return [
        { selector: ConnectorCanvasSelectors.selectLabels, value: [] },
        { selector: ConnectorCanvasSelectors.selectProcessors, value: [] },
        { selector: ConnectorCanvasSelectors.selectFunnels, value: [] },
        { selector: ConnectorCanvasSelectors.selectAllPorts, value: [] },
        { selector: ConnectorCanvasSelectors.selectRemoteProcessGroups, value: [] },
        { selector: ConnectorCanvasSelectors.selectProcessGroups, value: [] },
        { selector: ConnectorCanvasSelectors.selectConnections, value: [] },
        { selector: ConnectorCanvasSelectors.selectRegistryClients, value: [] },
        { selector: ConnectorCanvasSelectors.selectLoadingStatus, value: loadingStatus },
        { selector: ConnectorCanvasSelectors.selectSkipTransform, value: skipTransform },
        { selector: ConnectorCanvasSelectors.selectConnectorIdFromRoute, value: connectorId },
        { selector: ConnectorCanvasSelectors.selectProcessGroupIdFromRoute, value: processGroupId },
        { selector: selectParentProcessGroupId, value: parentProcessGroupId },
        { selector: selectUrl, value: url },
        { selector: selectRouteParams, value: routeParamsValue }
    ];
}

function configureConnectorCanvasTestBed(options: SetupOptions = {}) {
    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        imports: [ConnectorCanvasComponent, NoopAnimationsModule, MatDialogModule],
        providers: [provideRouter([]), provideMockStore({ initialState: {}, selectors: buildMockSelectors(options) })]
    }).overrideComponent(ConnectorCanvasComponent, {
        remove: { imports: [CanvasComponent, Navigation] },
        add: { imports: [MockReusableCanvasComponent, MockNavigationComponent] }
    });
}

function createConnectorCanvasFixture(): {
    fixture: ComponentFixture<ConnectorCanvasComponent>;
    component: ConnectorCanvasComponent;
    dispatchSpy: ReturnType<typeof vi.spyOn>;
} {
    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch');
    const fixture = TestBed.createComponent(ConnectorCanvasComponent);
    return { fixture, component: fixture.componentInstance, dispatchSpy };
}

function setup(options: SetupOptions = {}) {
    configureConnectorCanvasTestBed(options);
    return createConnectorCanvasFixture();
}

function dispatchWindowKeydown(key: string, modifiers: { ctrlKey?: boolean; metaKey?: boolean } = {}) {
    const event = new KeyboardEvent('keydown', { key, bubbles: true, ...modifiers });
    Object.defineProperty(event, 'target', { value: document.body, configurable: true });
    window.dispatchEvent(event);
}

describe('ConnectorCanvasComponent', () => {
    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('Component initialization', () => {
        it('should create', () => {
            const { component } = setup();
            expect(component).toBeTruthy();
        });

        it('should dispatch setConfiguration on init', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();

            expect(dispatchSpy).toHaveBeenCalledWith(
                setConfiguration({
                    configuration: {
                        features: { canEdit: false, canSelect: true }
                    }
                })
            );
        }));

        it('should dispatch loadConnectorFlow when route params are available', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadConnectorFlow({
                    connectorId: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID
                })
            );
        }));

        it('should NOT dispatch loadConnectorFlow when URL contains controller-services', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup({
                url: `/connectors/${DEFAULT_CONNECTOR_ID}/canvas/${DEFAULT_PROCESS_GROUP_ID}/controller-services`
            });
            fixture.detectChanges();
            tick();

            // Should only dispatch setConfiguration, NOT loadConnectorFlow
            const loadFlowDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === loadConnectorFlow.type
            );
            expect(loadFlowDispatches).toHaveLength(0);
        }));
    });

    describe('Component destruction', () => {
        it('should dispatch resetConnectorCanvasState on destroy', () => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            dispatchSpy.mockClear();

            fixture.destroy();

            expect(dispatchSpy).toHaveBeenCalledWith(resetConnectorCanvasState());
        });

        it('should dispatch resetConnectorCanvasEntityState on destroy', () => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            dispatchSpy.mockClear();

            fixture.destroy();

            expect(dispatchSpy).toHaveBeenCalledWith(resetConnectorCanvasEntityState());
        });
    });

    describe('Data ready state', () => {
        it('should emit true when loading status is success', async () => {
            const { component } = setup({ loadingStatus: 'success' });
            const ready = await firstValueFrom(component.dataReady$.pipe(take(1)));
            expect(ready).toBe(true);
        });

        it('should emit false when loading status is error', async () => {
            const { component } = setup({ loadingStatus: 'error' });
            const ready = await firstValueFrom(component.dataReady$.pipe(take(1)));
            expect(ready).toBe(false);
        });
    });

    describe('Error state', () => {
        it('should emit true when loading status is error', async () => {
            const { component } = setup({ loadingStatus: 'error' });
            const hasError = await firstValueFrom(component.hasError$.pipe(take(1)));
            expect(hasError).toBe(true);
        });

        it('should emit false when loading status is success', async () => {
            const { component } = setup({ loadingStatus: 'success' });
            const hasError = await firstValueFrom(component.hasError$.pipe(take(1)));
            expect(hasError).toBe(false);
        });

        it('should show error message (data-qa="not-found-state") when hasError is true', () => {
            const { fixture } = setup({ loadingStatus: 'error' });
            fixture.detectChanges();

            const notFound = fixture.nativeElement.querySelector('[data-qa="not-found-state"]');
            expect(notFound).not.toBeNull();
        });
    });

    describe('Process group navigation', () => {
        it('should dispatch enterProcessGroup when onProcessGroupDoubleClick is called', () => {
            const { component, dispatchSpy } = setup();
            dispatchSpy.mockClear();

            component.onProcessGroupDoubleClick({ processGroupId: 'nested-pg' });

            expect(dispatchSpy).toHaveBeenCalledWith(enterProcessGroup({ request: { id: 'nested-pg' } }));
        });

        it('should dispatch leaveProcessGroup when leaveGroupAction is called and canNavigateToParent is true', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({
                parentProcessGroupId: 'parent-pg'
            });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.leaveGroupAction();

            expect(dispatchSpy).toHaveBeenCalledWith(leaveProcessGroup());
        }));

        it('should NOT dispatch leaveProcessGroup when canNavigateToParent is false', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({
                parentProcessGroupId: null
            });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.leaveGroupAction();

            const leaveDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === leaveProcessGroup.type
            );
            expect(leaveDispatches).toHaveLength(0);
        }));
    });

    describe('Keyboard shortcuts', () => {
        it('should leave group on Escape when no dialog is open and canNavigateToParent is true', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({
                parentProcessGroupId: 'parent-pg'
            });
            fixture.detectChanges();
            tick();
            expect(component.canNavigateToParent).toBe(true);
            dispatchSpy.mockClear();

            dispatchWindowKeydown('Escape');

            expect(dispatchSpy).toHaveBeenCalledWith(leaveProcessGroup());
        }));

        it('should NOT leave group on Escape when dialog is open', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup({
                parentProcessGroupId: 'parent-pg'
            });
            fixture.detectChanges();
            tick();

            const dialog = TestBed.inject(MatDialog);
            dialog.open(MockBlockingDialogComponent);
            dispatchSpy.mockClear();

            dispatchWindowKeydown('Escape');

            const leaveDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === leaveProcessGroup.type
            );
            expect(leaveDispatches).toHaveLength(0);

            dialog.closeAll();
        }));

        it('should refresh on Ctrl+R when no dialog is open', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            dispatchWindowKeydown('r', { ctrlKey: true });

            expect(dispatchSpy).toHaveBeenCalledWith(
                loadConnectorFlow({
                    connectorId: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID
                })
            );
        }));

        it('should NOT refresh on Ctrl+R when dialog is open', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();

            const dialog = TestBed.inject(MatDialog);
            dialog.open(MockBlockingDialogComponent);
            dispatchSpy.mockClear();

            dispatchWindowKeydown('r', { ctrlKey: true });

            const loadFlowDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === loadConnectorFlow.type
            );
            expect(loadFlowDispatches).toHaveLength(0);

            dialog.closeAll();
        }));

        it('should NOT process keyboard events when target is a search-input', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup({
                parentProcessGroupId: 'parent-pg'
            });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            const searchEl = document.createElement('div');
            searchEl.classList.add('search-input');
            document.body.appendChild(searchEl);

            const escapeEvent = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true });
            searchEl.dispatchEvent(escapeEvent);

            const leaveDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === leaveProcessGroup.type
            );
            expect(leaveDispatches).toHaveLength(0);

            dispatchSpy.mockClear();
            const refreshEvent = new KeyboardEvent('keydown', { key: 'r', ctrlKey: true, bubbles: true });
            searchEl.dispatchEvent(refreshEvent);

            const loadFlowDispatches = dispatchSpy.mock.calls.filter(
                (call) => (call[0] as { type: string }).type === loadConnectorFlow.type
            );
            expect(loadFlowDispatches).toHaveLength(0);

            document.body.removeChild(searchEl);
        }));

        it('should NOT process keyboard events when target is an input element', fakeAsync(() => {
            const { fixture, dispatchSpy } = setup({
                parentProcessGroupId: 'parent-pg'
            });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            const inputEl = document.createElement('input');
            document.body.appendChild(inputEl);

            const escapeEvent = new KeyboardEvent('keydown', { key: 'Escape', bubbles: true });
            inputEl.dispatchEvent(escapeEvent);

            expect(
                dispatchSpy.mock.calls.filter((call) => (call[0] as { type: string }).type === leaveProcessGroup.type)
            ).toHaveLength(0);

            dispatchSpy.mockClear();
            const refreshEvent = new KeyboardEvent('keydown', { key: 'r', ctrlKey: true, bubbles: true });
            inputEl.dispatchEvent(refreshEvent);

            expect(
                dispatchSpy.mock.calls.filter((call) => (call[0] as { type: string }).type === loadConnectorFlow.type)
            ).toHaveLength(0);

            document.body.removeChild(inputEl);
        }));
    });

    describe('Selection routing', () => {
        it('should populate selectedComponentIds from route params with single selection', fakeAsync(() => {
            const { fixture, component } = setup({
                routeParams: {
                    id: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID,
                    type: ComponentType.Processor,
                    componentId: 'proc-1'
                }
            });
            fixture.detectChanges();
            tick();

            expect(component.selectedComponentIds).toEqual(['proc-1']);
        }));

        it('should populate selectedComponentIds from route params with bulk selection', fakeAsync(() => {
            const { fixture, component } = setup({
                routeParams: {
                    id: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID,
                    ids: 'proc-1,conn-2,pg-3'
                }
            });
            fixture.detectChanges();
            tick();

            expect(component.selectedComponentIds).toEqual(['proc-1', 'conn-2', 'pg-3']);
        }));

        it('should clear selectedComponentIds when no selection route params', fakeAsync(() => {
            const { fixture, component } = setup({
                routeParams: {
                    id: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID
                }
            });
            fixture.detectChanges();
            tick();

            expect(component.selectedComponentIds).toEqual([]);
        }));

        it('should dispatch selectComponents when onSelectComponents is called', () => {
            const { component, dispatchSpy } = setup();
            dispatchSpy.mockClear();

            component.onSelectComponents([{ id: 'proc-1', type: ComponentType.Processor }]);

            expect(dispatchSpy).toHaveBeenCalledWith(
                selectComponents({
                    request: {
                        components: [{ id: 'proc-1', componentType: ComponentType.Processor }]
                    }
                })
            );
        });

        it('should dispatch deselectAllComponents when onDeselectAll is called', () => {
            const { component, dispatchSpy } = setup();
            dispatchSpy.mockClear();

            component.onDeselectAll();

            expect(dispatchSpy).toHaveBeenCalledWith(deselectAllComponents());
        });
    });

    describe('Canvas initialized', () => {
        it('should clear skipTransform without centering when skipTransform is true and components are selected', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({
                skipTransform: true,
                routeParams: {
                    id: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID,
                    type: ComponentType.Processor,
                    componentId: 'proc-1'
                }
            });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.onCanvasInitialized();

            expect(dispatchSpy).toHaveBeenCalledWith(setSkipTransform({ skipTransform: false }));
        }));

        it('should not dispatch anything when no components are selected', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.onCanvasInitialized();

            expect(dispatchSpy).not.toHaveBeenCalled();
        }));
    });
});
