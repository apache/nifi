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
import { provideRouter, Router } from '@angular/router';
import { MatDialog, MatDialogModule } from '@angular/material/dialog';
import { provideMockActions } from '@ngrx/effects/testing';
import { Action } from '@ngrx/store';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { ReplaySubject, firstValueFrom } from 'rxjs';
import { take } from 'rxjs/operators';
import { ComponentType, selectRouteParams, selectUrl } from '@nifi/shared';

import { ConnectorCanvasComponent } from './connector-canvas.component';
import { CanvasComponent } from '../../../../ui/common/canvas/canvas.component';
import { ContextMenuContext } from '../../../../ui/common/canvas/canvas.types';
import { Navigation } from '../../../../ui/common/navigation/navigation.component';
import { ConnectorCanvasHeaderBarComponent } from './header-bar/connector-canvas-header-bar.component';
import { ConnectorCanvasFooterComponent } from './footer/footer.component';
import { selectCurrentUser } from '../../../../state/current-user/current-user.selectors';
import { ConnectorGraphControls } from './graph-controls/connector-graph-controls.component';
import { ContextErrorBanner } from '../../../../ui/common/context-error-banner/context-error-banner.component';
import { ErrorContextKey } from '../../../../state/error';
import { Storage } from '@nifi/shared';
import { setConfiguration } from '../../../../state/canvas-ui/canvas-ui.actions';
import * as ConnectorCanvasSelectors from '../../state/connector-canvas/connector-canvas.selectors';
import {
    selectConnectorCanvasEntity,
    selectConnectorCanvasEntitySaving
} from '../../state/connector-canvas-entity/connector-canvas-entity.selectors';
import { selectParentProcessGroupId } from '../../state/connector-canvas/connector-canvas.selectors';
import {
    deselectAllComponents,
    enterProcessGroup,
    leaveProcessGroup,
    loadConnectorFlow,
    loadConnectorFlowSuccess,
    navigateToControllerService,
    navigateToControllerServices,
    navigateToProvenanceForComponent,
    navigateToQueueListing,
    resetConnectorCanvasState,
    selectComponents,
    setSkipTransform,
    viewComponentConfiguration
} from '../../state/connector-canvas/connector-canvas.actions';
import {
    cancelConnectorDrain,
    promptDrainConnector,
    resetConnectorCanvasEntityState
} from '../../state/connector-canvas-entity/connector-canvas-entity.actions';
import { promptEmptyQueueRequest, promptEmptyQueuesRequest } from '../../../../state/empty-queue/empty-queue.actions';
import { getComponentStateAndOpenDialog } from '../../../../state/component-state/component-state.actions';

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
    menuProvider = input<unknown>(undefined);
    selectComponents = output<Array<{ id: string; type: ComponentType }>>();
    deselectAll = output<void>();
    initialized = output<void>();
    contextMenuOpened = output<unknown>();
    centerOnSelection = vi.fn();
    centerOnComponent = vi.fn();
    getBirdseyeComponentData = vi.fn().mockReturnValue([]);
    getCanvasDimensions = vi.fn().mockReturnValue({ width: 1024, height: 768 });
    onZoomIn = vi.fn();
    onZoomOut = vi.fn();
    onZoomFit = vi.fn();
    onZoomActual = vi.fn();
    setViewportPosition = vi.fn();
    birdseyeDragStart = vi.fn();
    birdseyeDragEnd = vi.fn();
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
    selector: 'connector-canvas-header-bar',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockConnectorCanvasHeaderBarComponent {
    connectorId = input.required<string>();
    selectedComponentId = input<string | null>(null);
    graphControlsOpen = input<boolean>(true);
    backToConnectors = output<void>();
    goToComponent = output<{ id: string; type: ComponentType; groupId: string }>();
    toggleGraphControls = output<void>();
}

@Component({
    selector: 'connector-canvas-footer',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockConnectorCanvasFooterComponent {
    connectorId = input.required<string>();
}

@Component({
    selector: 'connector-graph-controls',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockConnectorGraphControls {
    connectorEntity = input<unknown>(null);
    entitySaving = input<boolean>(false);
    birdseyeComponents = input<unknown[]>([]);
    birdseyeTransform = input<unknown>({ translate: { x: 0, y: 0 }, scale: 1 });
    canvasDimensions = input<unknown>({ width: 0, height: 0 });
    canNavigateToParent = input<boolean>(false);
    viewportChange = output<{ x: number; y: number }>();
    birdseyeDragStart = output<void>();
    birdseyeDragEnd = output<void>();
    zoomIn = output<void>();
    zoomOut = output<void>();
    zoomFit = output<void>();
    zoomActual = output<void>();
    leaveGroup = output<void>();
}

@Component({
    selector: 'context-error-banner',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockContextErrorBanner {
    context = input.required<ErrorContextKey>();
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
    canAccessProvenance?: boolean;
}

const DEFAULT_CONNECTOR_ID = 'connector-1';
const DEFAULT_PROCESS_GROUP_ID = 'pg-root';

function buildMockCurrentUser(canAccessProvenance: boolean) {
    const permissions = { canRead: false, canWrite: false };
    return {
        identity: 'test-user',
        anonymous: false,
        canVersionFlows: false,
        logoutSupported: false,
        provenancePermissions: { canRead: canAccessProvenance, canWrite: false },
        countersPermissions: permissions,
        tenantsPermissions: permissions,
        controllerPermissions: permissions,
        policiesPermissions: permissions,
        systemPermissions: permissions,
        parameterContextPermissions: permissions,
        connectorsPermissions: permissions,
        restrictedComponentsPermissions: permissions,
        componentRestrictionPermissions: []
    };
}

function buildMockSelectors(options: SetupOptions = {}) {
    const loadingStatus = options.loadingStatus ?? 'success';
    const connectorId = options.connectorId !== undefined ? options.connectorId : DEFAULT_CONNECTOR_ID;
    const processGroupId = options.processGroupId !== undefined ? options.processGroupId : DEFAULT_PROCESS_GROUP_ID;
    const url = options.url ?? `/connectors/${connectorId}/canvas/${processGroupId}`;
    const parentProcessGroupId = options.parentProcessGroupId !== undefined ? options.parentProcessGroupId : null;
    const skipTransform = options.skipTransform ?? false;
    const routeParamsValue = options.routeParams ?? { id: connectorId, processGroupId };
    const canAccessProvenance = options.canAccessProvenance ?? true;

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
        { selector: selectRouteParams, value: routeParamsValue },
        { selector: selectCurrentUser, value: buildMockCurrentUser(canAccessProvenance) },
        { selector: selectConnectorCanvasEntity, value: null },
        { selector: selectConnectorCanvasEntitySaving, value: false }
    ];
}

function createMockStorage() {
    const backingStore = new Map<string, unknown>();
    return {
        getItem: vi.fn((key: string) => backingStore.get(key) ?? null),
        setItem: vi.fn((key: string, value: unknown) => {
            backingStore.set(key, value);
        }),
        removeItem: vi.fn((key: string) => {
            backingStore.delete(key);
        }),
        hasItem: vi.fn((key: string) => backingStore.has(key)),
        getItemExpiration: vi.fn().mockReturnValue(null),
        __store: backingStore
    };
}

let actions$: ReplaySubject<Action>;

function configureConnectorCanvasTestBed(options: SetupOptions = {}, storage?: ReturnType<typeof createMockStorage>) {
    TestBed.resetTestingModule();
    const storageMock = storage ?? createMockStorage();
    actions$ = new ReplaySubject<Action>(1);
    TestBed.configureTestingModule({
        imports: [ConnectorCanvasComponent, NoopAnimationsModule, MatDialogModule],
        providers: [
            provideRouter([]),
            provideMockActions(() => actions$),
            provideMockStore({ initialState: {}, selectors: buildMockSelectors(options) }),
            { provide: Storage, useValue: storageMock }
        ]
    }).overrideComponent(ConnectorCanvasComponent, {
        remove: {
            imports: [
                CanvasComponent,
                Navigation,
                ConnectorCanvasHeaderBarComponent,
                ConnectorCanvasFooterComponent,
                ConnectorGraphControls,
                ContextErrorBanner
            ]
        },
        add: {
            imports: [
                MockReusableCanvasComponent,
                MockNavigationComponent,
                MockConnectorCanvasHeaderBarComponent,
                MockConnectorCanvasFooterComponent,
                MockConnectorGraphControls,
                MockContextErrorBanner
            ]
        }
    });
    return storageMock;
}

function createConnectorCanvasFixture(storage: ReturnType<typeof createMockStorage>): {
    fixture: ComponentFixture<ConnectorCanvasComponent>;
    component: ConnectorCanvasComponent;
    dispatchSpy: ReturnType<typeof vi.spyOn>;
    storage: ReturnType<typeof createMockStorage>;
} {
    const store = TestBed.inject(MockStore);
    const dispatchSpy = vi.spyOn(store, 'dispatch');
    const fixture = TestBed.createComponent(ConnectorCanvasComponent);
    return { fixture, component: fixture.componentInstance, dispatchSpy, storage };
}

function setup(options: SetupOptions = {}, storageOverride?: ReturnType<typeof createMockStorage>) {
    const storage = configureConnectorCanvasTestBed(options, storageOverride);
    return createConnectorCanvasFixture(storage);
}

function dispatchWindowKeydown(key: string, modifiers: { ctrlKey?: boolean; metaKey?: boolean } = {}) {
    const event = new KeyboardEvent('keydown', { key, bubbles: true, ...modifiers });
    Object.defineProperty(event, 'target', { value: document.body, configurable: true });
    window.dispatchEvent(event);
}

function getCanvasMock(fixture: ComponentFixture<ConnectorCanvasComponent>): MockReusableCanvasComponent {
    return fixture.debugElement.query((el) => el.name === 'reusable-canvas')
        .componentInstance as MockReusableCanvasComponent;
}

// viewChild.required(CanvasComponent) cannot resolve to the test mock because the mock has a
// different class identity than the real CanvasComponent. Stubbing the signal lets the SUT's
// birdseye / navigation handlers call into the mock directly.
function attachCanvasMock(component: ConnectorCanvasComponent, mock: MockReusableCanvasComponent): void {
    Object.defineProperty(component, 'canvasComponent', {
        configurable: true,
        value: () => mock
    });
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
                (call: unknown[]) => (call[0] as { type: string }).type === loadConnectorFlow.type
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
                (call: unknown[]) => (call[0] as { type: string }).type === leaveProcessGroup.type
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
                (call: unknown[]) => (call[0] as { type: string }).type === leaveProcessGroup.type
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
                (call: unknown[]) => (call[0] as { type: string }).type === loadConnectorFlow.type
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
                (call: unknown[]) => (call[0] as { type: string }).type === leaveProcessGroup.type
            );
            expect(leaveDispatches).toHaveLength(0);

            dispatchSpy.mockClear();
            const refreshEvent = new KeyboardEvent('keydown', { key: 'r', ctrlKey: true, bubbles: true });
            searchEl.dispatchEvent(refreshEvent);

            const loadFlowDispatches = dispatchSpy.mock.calls.filter(
                (call: unknown[]) => (call[0] as { type: string }).type === loadConnectorFlow.type
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
                dispatchSpy.mock.calls.filter(
                    (call: unknown[]) => (call[0] as { type: string }).type === leaveProcessGroup.type
                )
            ).toHaveLength(0);

            dispatchSpy.mockClear();
            const refreshEvent = new KeyboardEvent('keydown', { key: 'r', ctrlKey: true, bubbles: true });
            inputEl.dispatchEvent(refreshEvent);

            expect(
                dispatchSpy.mock.calls.filter(
                    (call: unknown[]) => (call[0] as { type: string }).type === loadConnectorFlow.type
                )
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
            attachCanvasMock(component, getCanvasMock(fixture));
            dispatchSpy.mockClear();

            component.onCanvasInitialized();

            expect(dispatchSpy).toHaveBeenCalledWith(setSkipTransform({ skipTransform: false }));
        }));

        it('should not dispatch anything when no components are selected', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            attachCanvasMock(component, getCanvasMock(fixture));
            dispatchSpy.mockClear();

            component.onCanvasInitialized();

            expect(dispatchSpy).not.toHaveBeenCalled();
        }));
    });

    describe('Header bar and footer', () => {
        it('should render the header bar component', fakeAsync(() => {
            const { fixture } = setup();
            fixture.detectChanges();
            tick();

            const headerBar = fixture.nativeElement.querySelector('connector-canvas-header-bar');
            expect(headerBar).toBeTruthy();
        }));

        it('should render the footer component', fakeAsync(() => {
            const { fixture } = setup();
            fixture.detectChanges();
            tick();

            const footer = fixture.nativeElement.querySelector('connector-canvas-footer');
            expect(footer).toBeTruthy();
        }));
    });

    describe('Graph controls toggle', () => {
        it('should default graphControlsOpen to true when storage has no entry', () => {
            const { component } = setup();
            expect(component.graphControlsOpen).toBe(true);
        });

        it('should restore graphControlsOpen from Storage service when persisted as false', () => {
            const storage = createMockStorage();
            storage.getItem.mockImplementation((key: string) =>
                key === 'graph-control-visibility' ? { 'connector-graph-controls': false } : null
            );
            const { component } = setup({}, storage);
            expect(component.graphControlsOpen).toBe(false);
        });

        it('should toggle graphControlsOpen and persist state via Storage service', () => {
            const { component, storage } = setup();

            component.toggleGraphControls();
            expect(component.graphControlsOpen).toBe(false);
            expect(storage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({ 'connector-graph-controls': false })
            );

            component.toggleGraphControls();
            expect(component.graphControlsOpen).toBe(true);
            expect(storage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({ 'connector-graph-controls': true })
            );
        });

        it('should preserve sibling visibility keys when toggling', () => {
            const storage = createMockStorage();
            storage.getItem.mockImplementation((key: string) =>
                key === 'graph-control-visibility' ? { 'other-control': true, 'connector-graph-controls': true } : null
            );
            const { component } = setup({}, storage);

            component.toggleGraphControls();

            expect(storage.setItem).toHaveBeenCalledWith(
                'graph-control-visibility',
                expect.objectContaining({
                    'other-control': true,
                    'connector-graph-controls': false
                })
            );
        });
    });

    describe('Search navigation', () => {
        it('should select components when search result is in the current process group', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({ processGroupId: DEFAULT_PROCESS_GROUP_ID });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            const selectSpy = vi.spyOn(component, 'onSelectComponents');
            try {
                component.onSearchGoToComponent({
                    id: 'proc-1',
                    type: ComponentType.Processor,
                    groupId: DEFAULT_PROCESS_GROUP_ID
                });
            } catch (_e: unknown) {
                // NG0951: viewChild.required for CanvasComponent is not resolvable in tests with mock overrides
            }

            expect(selectSpy).toHaveBeenCalledWith([{ id: 'proc-1', type: ComponentType.Processor }]);
            expect(dispatchSpy).toHaveBeenCalledWith(
                selectComponents({
                    request: {
                        components: [{ id: 'proc-1', componentType: ComponentType.Processor }]
                    }
                })
            );
        }));

        it('should navigate to settings when search result is a ParameterProvider', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            const router = TestBed.inject(Router);
            const navigateSpy = vi.spyOn(router, 'navigate');

            component.onSearchGoToComponent({
                id: 'pp-1',
                type: ComponentType.ParameterProvider,
                groupId: DEFAULT_PROCESS_GROUP_ID
            });

            expect(navigateSpy).toHaveBeenCalledWith(['/settings', 'parameter-providers', 'pp-1']);
            expect(dispatchSpy).not.toHaveBeenCalled();
        }));

        it('should dispatch navigateToControllerService when search result is a ControllerService', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.onSearchGoToComponent({
                id: 'svc-1',
                type: ComponentType.ControllerService,
                groupId: 'pg-target'
            });

            expect(dispatchSpy).toHaveBeenCalledWith(
                navigateToControllerService({ processGroupId: 'pg-target', serviceId: 'svc-1' })
            );
        }));

        it('should dispatch skipTransform and navigate when search result is in a different process group', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup();
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            const router = TestBed.inject(Router);
            const navigateSpy = vi.spyOn(router, 'navigate');

            component.onSearchGoToComponent({
                id: 'proc-2',
                type: ComponentType.Processor,
                groupId: 'different-pg'
            });

            expect(dispatchSpy).toHaveBeenCalledWith(setSkipTransform({ skipTransform: false }));
            expect(navigateSpy).toHaveBeenCalledWith([
                '/connectors',
                DEFAULT_CONNECTOR_ID,
                'canvas',
                'different-pg',
                ComponentType.Processor,
                'proc-2'
            ]);
        }));
    });

    describe('Context menu provider', () => {
        function buildCanvasContext(): ContextMenuContext {
            return {
                processGroupId: DEFAULT_PROCESS_GROUP_ID,
                targetType: 'canvas',
                selectedComponents: [],
                allConnections: []
            };
        }

        function buildComponentContext(
            componentType: ComponentType,
            entityId: string,
            selectedCount = 1,
            entityOverrides: Record<string, any> = {}
        ): ContextMenuContext {
            const component = {
                ui: { componentType } as any,
                entity: { id: entityId, permissions: { canRead: true, canWrite: true }, ...entityOverrides } as any
            };
            return {
                processGroupId: DEFAULT_PROCESS_GROUP_ID,
                targetType: 'component',
                clickedComponent: component,
                selectedComponents: Array.from({ length: selectedCount }, () => component),
                allConnections: []
            };
        }

        describe('getMenu', () => {
            it('should return undefined for non-root menuId', () => {
                const { component } = setup();
                const result = component.contextMenuProvider.getMenu('other');
                expect(result).toBeUndefined();
            });

            it('should return canvas menu with Refresh, Leave Group, and Empty All Queues when targetType is canvas', fakeAsync(() => {
                const { fixture, component } = setup({ parentProcessGroupId: 'parent-pg' });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());

                const menu = component.contextMenuProvider.getMenu('root');
                expect(menu).toBeDefined();

                const items = menu!.menuItems.filter((item) => !item.isSeparator);
                expect(items.map((i) => i.text)).toEqual([
                    'Refresh',
                    'Leave Group',
                    'Controller Services',
                    'Empty All Queues',
                    'Drain',
                    'Cancel Drain'
                ]);
            }));

            it('should return component menu with all defined items when targetType is component', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1'));

                const menu = component.contextMenuProvider.getMenu('root');
                expect(menu).toBeDefined();

                const items = menu!.menuItems.filter((item) => !item.isSeparator);
                expect(items.map((i) => i.text)).toEqual([
                    'View Configuration',
                    'Enter Group',
                    'View Data Provenance',
                    'View State',
                    'List Queue',
                    'Empty Queue',
                    'Empty All Queues',
                    'Controller Services',
                    'Center In View'
                ]);
            }));

            it('should return empty menu when no context is set', () => {
                const { component } = setup();
                const menu = component.contextMenuProvider.getMenu('root');
                expect(menu).toBeDefined();
                expect(menu!.menuItems).toEqual([]);
            });
        });

        describe('canvas menu conditions', () => {
            it('should always show Refresh', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const refresh = menu.menuItems.find((i) => i.text === 'Refresh');

                expect(refresh).toBeDefined();
                expect(refresh!.condition!(null)).toBe(true);
            }));

            it('should show Leave Group only when canNavigateToParent is true', fakeAsync(() => {
                const { fixture, component } = setup({ parentProcessGroupId: 'parent-pg' });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const leaveGroup = menu.menuItems.find((i) => i.text === 'Leave Group');

                expect(leaveGroup).toBeDefined();
                expect(leaveGroup!.condition!(null)).toBe(true);
            }));

            it('should hide Leave Group when canNavigateToParent is false', fakeAsync(() => {
                const { fixture, component } = setup({ parentProcessGroupId: null });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const leaveGroup = menu.menuItems.find((i) => i.text === 'Leave Group');

                expect(leaveGroup).toBeDefined();
                expect(leaveGroup!.condition!(null)).toBe(false);
            }));

            it('should show Controller Services when a process group context is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const controllerServices = menu.menuItems.find((i) => i.text === 'Controller Services');

                expect(controllerServices).toBeDefined();
                expect(controllerServices!.condition!(null)).toBe(true);
            }));

            it('should hide Controller Services when no process group is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();
                component.currentProcessGroupId = null;

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const controllerServices = menu.menuItems.find((i) => i.text === 'Controller Services');

                expect(controllerServices).toBeDefined();
                expect(controllerServices!.condition!(null)).toBe(false);
            }));

            it('should show Empty All Queues when a process group context is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyAll = menu.menuItems.find((i) => i.text === 'Empty All Queues');

                expect(emptyAll).toBeDefined();
                expect(emptyAll!.condition!(null)).toBe(true);
            }));

            it('should hide Empty All Queues when no process group is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();
                component.currentProcessGroupId = null;

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyAll = menu.menuItems.find((i) => i.text === 'Empty All Queues');

                expect(emptyAll).toBeDefined();
                expect(emptyAll!.condition!(null)).toBe(false);
            }));

            it('should show Drain when the connector entity allows DRAIN_FLOWFILES', fakeAsync(() => {
                const { fixture, component } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, {
                    id: 'connector-1',
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canRead: true, canWrite: true },
                    component: {
                        availableActions: [{ name: 'DRAIN_FLOWFILES', allowed: true }]
                    }
                } as any);
                store.refreshState();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const drain = menu.menuItems.find((i) => i.text === 'Drain');

                expect(drain).toBeDefined();
                expect(drain!.condition!(null)).toBe(true);
            }));

            it('should hide Drain when no entity is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const drain = menu.menuItems.find((i) => i.text === 'Drain');

                expect(drain).toBeDefined();
                expect(drain!.condition!(null)).toBe(false);
            }));

            it('should hide Drain while the entity is saving', fakeAsync(() => {
                const { fixture, component } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, {
                    id: 'connector-1',
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canRead: true, canWrite: true },
                    component: {
                        availableActions: [{ name: 'DRAIN_FLOWFILES', allowed: true }]
                    }
                } as any);
                store.overrideSelector(selectConnectorCanvasEntitySaving, true);
                store.refreshState();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const drain = menu.menuItems.find((i) => i.text === 'Drain');

                expect(drain).toBeDefined();
                expect(drain!.condition!(null)).toBe(false);
            }));

            it('should show Cancel Drain when the connector entity allows CANCEL_DRAIN_FLOWFILES', fakeAsync(() => {
                const { fixture, component } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, {
                    id: 'connector-1',
                    permissions: { canRead: true, canWrite: true },
                    operatePermissions: { canRead: true, canWrite: true },
                    component: {
                        availableActions: [{ name: 'CANCEL_DRAIN_FLOWFILES', allowed: true }]
                    }
                } as any);
                store.refreshState();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const cancelDrain = menu.menuItems.find((i) => i.text === 'Cancel Drain');

                expect(cancelDrain).toBeDefined();
                expect(cancelDrain!.condition!(null)).toBe(true);
            }));

            it('should hide Cancel Drain when no entity is loaded', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildCanvasContext());
                const menu = component.contextMenuProvider.getMenu('root')!;
                const cancelDrain = menu.menuItems.find((i) => i.text === 'Cancel Drain');

                expect(cancelDrain).toBeDefined();
                expect(cancelDrain!.condition!(null)).toBe(false);
            }));
        });

        describe('component menu conditions', () => {
            it('should show Enter Group for ProcessGroup with single selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const enterGroup = menu.menuItems.find((i) => i.text === 'Enter Group');

                expect(enterGroup).toBeDefined();
                expect(enterGroup!.condition!(null)).toBe(true);
            }));

            it('should hide Enter Group for non-ProcessGroup types', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const enterGroup = menu.menuItems.find((i) => i.text === 'Enter Group');

                expect(enterGroup).toBeDefined();
                expect(enterGroup!.condition!(null)).toBe(false);
            }));

            it('should hide Enter Group for multi-selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 3));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const enterGroup = menu.menuItems.find((i) => i.text === 'Enter Group');

                expect(enterGroup).toBeDefined();
                expect(enterGroup!.condition!(null)).toBe(false);
            }));

            it('should always show Center In View', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const center = menu.menuItems.find((i) => i.text === 'Center In View');

                expect(center).toBeDefined();
                expect(center!.condition!(null)).toBe(true);
            }));

            it('should show View Data Provenance for Processor with provenance access', fakeAsync(() => {
                const { fixture, component } = setup({ canAccessProvenance: true });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const provenance = menu.menuItems.find((i) => i.text === 'View Data Provenance');

                expect(provenance).toBeDefined();
                expect(provenance!.condition!(null)).toBe(true);
            }));

            it('should hide View Data Provenance when user lacks provenance access', fakeAsync(() => {
                const { fixture, component } = setup({ canAccessProvenance: false });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const provenance = menu.menuItems.find((i) => i.text === 'View Data Provenance');

                expect(provenance).toBeDefined();
                expect(provenance!.condition!(null)).toBe(false);
            }));

            it('should hide View Data Provenance for ProcessGroup', fakeAsync(() => {
                const { fixture, component } = setup({ canAccessProvenance: true });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const provenance = menu.menuItems.find((i) => i.text === 'View Data Provenance');

                expect(provenance).toBeDefined();
                expect(provenance!.condition!(null)).toBe(false);
            }));

            it('should hide View Data Provenance for Connection', fakeAsync(() => {
                const { fixture, component } = setup({ canAccessProvenance: true });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Connection, 'conn-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const provenance = menu.menuItems.find((i) => i.text === 'View Data Provenance');

                expect(provenance).toBeDefined();
                expect(provenance!.condition!(null)).toBe(false);
            }));

            it('should hide View Data Provenance for multi-selection', fakeAsync(() => {
                const { fixture, component } = setup({ canAccessProvenance: true });
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 3));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const provenance = menu.menuItems.find((i) => i.text === 'View Data Provenance');

                expect(provenance).toBeDefined();
                expect(provenance!.condition!(null)).toBe(false);
            }));

            it('should show View State for a stateful Processor with read/write permissions', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.Processor, 'proc-1', 1, {
                        component: { persistsState: true, name: 'My Processor' },
                        permissions: { canRead: true, canWrite: true }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewState = menu.menuItems.find((i) => i.text === 'View State');

                expect(viewState).toBeDefined();
                expect(viewState!.condition!(null)).toBe(true);
            }));

            it('should hide View State for non-Processor types', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 1, {
                        component: { persistsState: true }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewState = menu.menuItems.find((i) => i.text === 'View State');

                expect(viewState).toBeDefined();
                expect(viewState!.condition!(null)).toBe(false);
            }));

            it('should hide View State when processor does not persist state', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.Processor, 'proc-1', 1, {
                        component: { persistsState: false, name: 'Stateless Proc' },
                        permissions: { canRead: true, canWrite: true }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewState = menu.menuItems.find((i) => i.text === 'View State');

                expect(viewState).toBeDefined();
                expect(viewState!.condition!(null)).toBe(false);
            }));

            it('should hide View State when user lacks write permission', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.Processor, 'proc-1', 1, {
                        component: { persistsState: true, name: 'My Processor' },
                        permissions: { canRead: true, canWrite: false }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewState = menu.menuItems.find((i) => i.text === 'View State');

                expect(viewState).toBeDefined();
                expect(viewState!.condition!(null)).toBe(false);
            }));

            it('should hide View State for multi-selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.Processor, 'proc-1', 3, {
                        component: { persistsState: true, name: 'My Processor' },
                        permissions: { canRead: true, canWrite: true }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewState = menu.menuItems.find((i) => i.text === 'View State');

                expect(viewState).toBeDefined();
                expect(viewState!.condition!(null)).toBe(false);
            }));

            it('should show View Configuration for a readable Processor with single selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewConfig = menu.menuItems.find((i) => i.text === 'View Configuration');

                expect(viewConfig).toBeDefined();
                expect(viewConfig!.condition!(null)).toBe(true);
            }));

            it('should hide View Configuration when user lacks read permission', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(
                    buildComponentContext(ComponentType.Processor, 'proc-1', 1, {
                        permissions: { canRead: false, canWrite: false }
                    })
                );
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewConfig = menu.menuItems.find((i) => i.text === 'View Configuration');

                expect(viewConfig).toBeDefined();
                expect(viewConfig!.condition!(null)).toBe(false);
            }));

            it('should hide View Configuration for Funnel components', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Funnel, 'funnel-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewConfig = menu.menuItems.find((i) => i.text === 'View Configuration');

                expect(viewConfig).toBeDefined();
                expect(viewConfig!.condition!(null)).toBe(false);
            }));

            it('should hide View Configuration for multi-selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 3));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const viewConfig = menu.menuItems.find((i) => i.text === 'View Configuration');

                expect(viewConfig).toBeDefined();
                expect(viewConfig!.condition!(null)).toBe(false);
            }));

            it('should show List Queue for a single Connection selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Connection, 'conn-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const listQueue = menu.menuItems.find((i) => i.text === 'List Queue');

                expect(listQueue).toBeDefined();
                expect(listQueue!.condition!(null)).toBe(true);
            }));

            it('should hide List Queue for non-Connection types', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const listQueue = menu.menuItems.find((i) => i.text === 'List Queue');

                expect(listQueue).toBeDefined();
                expect(listQueue!.condition!(null)).toBe(false);
            }));

            it('should hide List Queue for multi-selection of Connections', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Connection, 'conn-1', 2));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const listQueue = menu.menuItems.find((i) => i.text === 'List Queue');

                expect(listQueue).toBeDefined();
                expect(listQueue!.condition!(null)).toBe(false);
            }));

            it('should show Empty Queue for a single Connection selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Connection, 'conn-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyQueue = menu.menuItems.find((i) => i.text === 'Empty Queue');

                expect(emptyQueue).toBeDefined();
                expect(emptyQueue!.condition!(null)).toBe(true);
            }));

            it('should hide Empty Queue for non-Connection types', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyQueue = menu.menuItems.find((i) => i.text === 'Empty Queue');

                expect(emptyQueue).toBeDefined();
                expect(emptyQueue!.condition!(null)).toBe(false);
            }));

            it('should show Empty All Queues for a single ProcessGroup selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyAll = menu.menuItems.find((i) => i.text === 'Empty All Queues');

                expect(emptyAll).toBeDefined();
                expect(emptyAll!.condition!(null)).toBe(true);
            }));

            it('should hide Empty All Queues on a component menu when not a ProcessGroup', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyAll = menu.menuItems.find((i) => i.text === 'Empty All Queues');

                expect(emptyAll).toBeDefined();
                expect(emptyAll!.condition!(null)).toBe(false);
            }));

            it('should hide Empty All Queues when ProcessGroup is part of a multi-selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 3));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const emptyAll = menu.menuItems.find((i) => i.text === 'Empty All Queues');

                expect(emptyAll).toBeDefined();
                expect(emptyAll!.condition!(null)).toBe(false);
            }));

            it('should show Controller Services for a single ProcessGroup selection', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.ProcessGroup, 'pg-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const controllerServices = menu.menuItems.find((i) => i.text === 'Controller Services');

                expect(controllerServices).toBeDefined();
                expect(controllerServices!.condition!(null)).toBe(true);
            }));

            it('should hide Controller Services on a component menu when not a ProcessGroup', fakeAsync(() => {
                const { fixture, component } = setup();
                fixture.detectChanges();
                tick();

                component.onContextMenuOpened(buildComponentContext(ComponentType.Processor, 'proc-1', 1));
                const menu = component.contextMenuProvider.getMenu('root')!;
                const controllerServices = menu.menuItems.find((i) => i.text === 'Controller Services');

                expect(controllerServices).toBeDefined();
                expect(controllerServices!.condition!(null)).toBe(false);
            }));
        });

        describe('filterMenuItem', () => {
            it('should return true when condition returns true', () => {
                const { component } = setup();
                const item = { condition: () => true, text: 'test' };
                expect(component.contextMenuProvider.filterMenuItem(item)).toBe(true);
            });

            it('should return false when condition returns false', () => {
                const { component } = setup();
                const item = { condition: () => false, text: 'test' };
                expect(component.contextMenuProvider.filterMenuItem(item)).toBe(false);
            });

            it('should return true when no condition is set (separators)', () => {
                const { component } = setup();
                const item = { isSeparator: true };
                expect(component.contextMenuProvider.filterMenuItem(item)).toBe(true);
            });
        });

        describe('menuItemClicked', () => {
            it('should call the item action', () => {
                const { component } = setup();
                const action = vi.fn();
                const item = { text: 'test', action };
                const event = new MouseEvent('click');

                component.contextMenuProvider.menuItemClicked(item, event);

                expect(action).toHaveBeenCalled();
            });
        });

        describe('onContextMenuOpened', () => {
            it('should store the context', () => {
                const { component } = setup();
                const context = buildCanvasContext();

                component.onContextMenuOpened(context);

                const menu = component.contextMenuProvider.getMenu('root');
                expect(menu).toBeDefined();
                expect(menu!.menuItems.length).toBeGreaterThan(0);
            });
        });
    });

    describe('Action methods', () => {
        describe('refreshAction', () => {
            it('should dispatch loadConnectorFlow', fakeAsync(() => {
                const { fixture, dispatchSpy } = setup();
                fixture.detectChanges();
                tick();
                dispatchSpy.mockClear();

                fixture.componentInstance.refreshAction();

                expect(dispatchSpy).toHaveBeenCalledWith(
                    loadConnectorFlow({
                        connectorId: DEFAULT_CONNECTOR_ID,
                        processGroupId: DEFAULT_PROCESS_GROUP_ID
                    })
                );
            }));

            it('should not dispatch when connectorId is empty', () => {
                const { component, dispatchSpy } = setup();
                component.currentConnectorId = '';
                component.currentProcessGroupId = null;
                dispatchSpy.mockClear();

                component.refreshAction();

                expect(dispatchSpy).not.toHaveBeenCalled();
            });
        });

        describe('enterGroupAction', () => {
            it('should dispatch enterProcessGroup', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.enterGroupAction('pg-nested');

                expect(dispatchSpy).toHaveBeenCalledWith(enterProcessGroup({ request: { id: 'pg-nested' } }));
            });
        });

        describe('viewDataProvenanceAction', () => {
            it('should dispatch navigateToProvenanceForComponent', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.viewDataProvenanceAction('proc-1', ComponentType.Processor);

                expect(dispatchSpy).toHaveBeenCalledWith(
                    navigateToProvenanceForComponent({ id: 'proc-1', componentType: ComponentType.Processor })
                );
            });
        });

        describe('viewConfigurationAction', () => {
            it('should dispatch viewComponentConfiguration with the supplied entity and componentType', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();
                const entity = { id: 'proc-1', permissions: { canRead: true } };

                component.viewConfigurationAction(entity, ComponentType.Processor);

                expect(dispatchSpy).toHaveBeenCalledWith(
                    viewComponentConfiguration({
                        request: { entity, componentType: ComponentType.Processor }
                    })
                );
            });
        });

        describe('onComponentDoubleClick', () => {
            it('should dispatch viewComponentConfiguration when entity is readable', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();
                const entity = { id: 'proc-1', permissions: { canRead: true } };

                component.onComponentDoubleClick({ entity, componentType: ComponentType.Processor });

                expect(dispatchSpy).toHaveBeenCalledWith(
                    viewComponentConfiguration({
                        request: { entity, componentType: ComponentType.Processor }
                    })
                );
            });

            it('should not dispatch when entity is not readable', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();
                const entity = { id: 'proc-1', permissions: { canRead: false } };

                component.onComponentDoubleClick({ entity, componentType: ComponentType.Processor });

                expect(dispatchSpy).not.toHaveBeenCalled();
            });
        });

        describe('viewProcessorStateAction', () => {
            it('should dispatch getComponentStateAndOpenDialog with connectorId', fakeAsync(() => {
                const { fixture, component, dispatchSpy } = setup();
                fixture.detectChanges();
                tick();
                dispatchSpy.mockClear();

                const processorEntity = {
                    id: 'proc-1',
                    component: { name: 'My Processor', persistsState: true },
                    status: { aggregateSnapshot: { runStatus: 'Stopped', activeThreadCount: 0 } }
                };

                component.viewProcessorStateAction(processorEntity);

                expect(dispatchSpy).toHaveBeenCalledWith(
                    getComponentStateAndOpenDialog({
                        request: {
                            componentName: 'My Processor',
                            componentId: 'proc-1',
                            componentType: ComponentType.Processor,
                            canClear: true,
                            connectorId: DEFAULT_CONNECTOR_ID
                        }
                    })
                );
            }));

            it('should set canClear to false when processor is running', fakeAsync(() => {
                const { fixture, component, dispatchSpy } = setup();
                fixture.detectChanges();
                tick();
                dispatchSpy.mockClear();

                const processorEntity = {
                    id: 'proc-1',
                    component: { name: 'Running Processor', persistsState: true },
                    status: { aggregateSnapshot: { runStatus: 'Running', activeThreadCount: 1 } }
                };

                component.viewProcessorStateAction(processorEntity);

                expect(dispatchSpy).toHaveBeenCalledWith(
                    getComponentStateAndOpenDialog({
                        request: {
                            componentName: 'Running Processor',
                            componentId: 'proc-1',
                            componentType: ComponentType.Processor,
                            canClear: false,
                            connectorId: DEFAULT_CONNECTOR_ID
                        }
                    })
                );
            }));

            it('should set canClear to false when processor has active threads', fakeAsync(() => {
                const { fixture, component, dispatchSpy } = setup();
                fixture.detectChanges();
                tick();
                dispatchSpy.mockClear();

                const processorEntity = {
                    id: 'proc-1',
                    component: { name: 'Active Processor', persistsState: true },
                    status: { aggregateSnapshot: { runStatus: 'Stopped', activeThreadCount: 2 } }
                };

                component.viewProcessorStateAction(processorEntity);

                expect(dispatchSpy).toHaveBeenCalledWith(
                    getComponentStateAndOpenDialog({
                        request: {
                            componentName: 'Active Processor',
                            componentId: 'proc-1',
                            componentType: ComponentType.Processor,
                            canClear: false,
                            connectorId: DEFAULT_CONNECTOR_ID
                        }
                    })
                );
            }));
        });

        describe('queue and controller-services actions', () => {
            it('should dispatch navigateToQueueListing when listQueueAction is invoked', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.listQueueAction('conn-1');

                expect(dispatchSpy).toHaveBeenCalledWith(
                    navigateToQueueListing({ request: { connectionId: 'conn-1' } })
                );
            });

            it('should dispatch promptEmptyQueueRequest with connector-canvas source when emptyQueueAction is invoked', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.emptyQueueAction('conn-1');

                expect(dispatchSpy).toHaveBeenCalledWith(
                    promptEmptyQueueRequest({
                        request: { connectionId: 'conn-1', source: 'connector-canvas' }
                    })
                );
            });

            it('should dispatch promptEmptyQueuesRequest with connector-canvas source when emptyAllQueuesAction is invoked', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.emptyAllQueuesAction('pg-1');

                expect(dispatchSpy).toHaveBeenCalledWith(
                    promptEmptyQueuesRequest({
                        request: { processGroupId: 'pg-1', source: 'connector-canvas' }
                    })
                );
            });

            it('should dispatch navigateToControllerServices when controllerServicesAction is invoked', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.controllerServicesAction('pg-target');

                expect(dispatchSpy).toHaveBeenCalledWith(navigateToControllerServices({ processGroupId: 'pg-target' }));
            });
        });

        describe('drain actions', () => {
            const operableConnectorEntity = {
                id: 'connector-1',
                permissions: { canRead: true, canWrite: true },
                operatePermissions: { canRead: true, canWrite: true },
                component: {
                    availableActions: [
                        { name: 'DRAIN_FLOWFILES', allowed: true },
                        { name: 'CANCEL_DRAIN_FLOWFILES', allowed: true }
                    ]
                }
            };

            it('canDrain should be false when no entity is loaded', () => {
                const { component } = setup();
                expect(component.canDrain()).toBe(false);
            });

            it('canDrain should be false while the entity is saving', () => {
                const { component, fixture } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, operableConnectorEntity);
                store.overrideSelector(selectConnectorCanvasEntitySaving, true);
                store.refreshState();
                fixture.detectChanges();

                expect(component.canDrain()).toBe(false);
            });

            it('cancelDrainAction should not dispatch when no entity is loaded', () => {
                const { component, dispatchSpy } = setup();
                dispatchSpy.mockClear();

                component.cancelDrainConnectorAction();

                expect(dispatchSpy).not.toHaveBeenCalled();
            });

            it('drainConnectorAction should dispatch promptDrainConnector with the current entity', () => {
                const { component, fixture, dispatchSpy } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, operableConnectorEntity);
                store.refreshState();
                fixture.detectChanges();
                dispatchSpy.mockClear();

                component.drainConnectorAction();

                expect(dispatchSpy).toHaveBeenCalledWith(
                    promptDrainConnector({ connector: operableConnectorEntity as any })
                );
            });

            it('cancelDrainConnectorAction should dispatch cancelConnectorDrain with the current entity', () => {
                const { component, fixture, dispatchSpy } = setup();
                const store = TestBed.inject(MockStore);
                store.overrideSelector(selectConnectorCanvasEntity, operableConnectorEntity);
                store.refreshState();
                fixture.detectChanges();
                dispatchSpy.mockClear();

                component.cancelDrainConnectorAction();

                expect(dispatchSpy).toHaveBeenCalledWith(
                    cancelConnectorDrain({ connector: operableConnectorEntity as any })
                );
            });
        });
    });

    describe('Template bindings', () => {
        it('should pass menuProvider to reusable-canvas', fakeAsync(() => {
            const { fixture } = setup();
            fixture.detectChanges();
            tick();

            const canvasDebugEl = fixture.debugElement.query((el) => el.name === 'reusable-canvas');
            expect(canvasDebugEl).toBeTruthy();
            expect(canvasDebugEl.componentInstance.menuProvider()).toBeTruthy();
        }));
    });

    describe('Navigation control / birdseye wiring', () => {
        function getGraphControlsMock(fixture: ComponentFixture<ConnectorCanvasComponent>): MockConnectorGraphControls {
            return fixture.debugElement.query((el) => el.name === 'connector-graph-controls')
                .componentInstance as MockConnectorGraphControls;
        }

        it('should seed birdseyeComponents and canvasDimensions from the canvas on initialization', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);
            const seededComponents = [
                {
                    id: 'p1',
                    type: ComponentType.Processor,
                    position: { x: 1, y: 2 },
                    dimensions: { width: 10, height: 20 }
                }
            ];
            canvas.getBirdseyeComponentData.mockReturnValue(seededComponents);
            canvas.getCanvasDimensions.mockReturnValue({ width: 1024, height: 768 });

            component.onCanvasInitialized();

            expect(canvas.getBirdseyeComponentData).toHaveBeenCalled();
            expect(canvas.getCanvasDimensions).toHaveBeenCalled();
            expect(component.birdseyeComponents()).toEqual(seededComponents);
            expect(component.canvasDimensions()).toEqual({ width: 1024, height: 768 });
        }));

        it('should update birdseyeTransform when the canvas emits transformChange', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const transform = { translate: { x: -100, y: -50 }, scale: 0.5 };
            component.onTransformChange(transform);

            expect(component.birdseyeTransform()).toEqual(transform);
        }));

        it('should refresh birdseye components when loadConnectorFlowSuccess is dispatched', async () => {
            const { fixture, component } = setup();
            fixture.detectChanges();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);
            component.onCanvasInitialized();

            const refreshedComponents = [
                {
                    id: 'p2',
                    type: ComponentType.Processor,
                    position: { x: 5, y: 6 },
                    dimensions: { width: 30, height: 40 }
                }
            ];
            canvas.getBirdseyeComponentData.mockReturnValue(refreshedComponents);

            actions$.next(
                loadConnectorFlowSuccess({
                    connectorId: DEFAULT_CONNECTOR_ID,
                    processGroupId: DEFAULT_PROCESS_GROUP_ID,
                    parentProcessGroupId: null,
                    breadcrumb: null,
                    labels: [],
                    funnels: [],
                    inputPorts: [],
                    outputPorts: [],
                    remoteProcessGroups: [],
                    processGroups: [],
                    processors: [],
                    connections: []
                })
            );

            // queueMicrotask defers the refresh; await a microtask to let it run
            await Promise.resolve();

            expect(component.birdseyeComponents()).toEqual(refreshedComponents);
        });

        it('should delegate zoom buttons to the canvas', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);

            component.onNavigationZoomIn();
            component.onNavigationZoomOut();
            component.onNavigationZoomFit();
            component.onNavigationZoomActual();

            expect(canvas.onZoomIn).toHaveBeenCalledTimes(1);
            expect(canvas.onZoomOut).toHaveBeenCalledTimes(1);
            expect(canvas.onZoomFit).toHaveBeenCalledTimes(1);
            expect(canvas.onZoomActual).toHaveBeenCalledTimes(1);
        }));

        it('should delegate leave group via leaveGroupAction', fakeAsync(() => {
            const { fixture, component, dispatchSpy } = setup({ parentProcessGroupId: 'parent-pg' });
            fixture.detectChanges();
            tick();
            dispatchSpy.mockClear();

            component.onNavigationLeaveGroup();

            expect(dispatchSpy).toHaveBeenCalledWith(leaveProcessGroup());
        }));

        it('should delegate birdseye viewport change to the canvas', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);

            component.onBirdseyeViewportChange({ x: 42, y: 84 });

            expect(canvas.setViewportPosition).toHaveBeenCalledWith(42, 84, false);
        }));

        it('should delegate birdseye drag start/end to the canvas', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);

            component.onBirdseyeDragStart();
            component.onBirdseyeDragEnd();

            expect(canvas.birdseyeDragStart).toHaveBeenCalledTimes(1);
            expect(canvas.birdseyeDragEnd).toHaveBeenCalledTimes(1);
        }));

        it('should refresh canvasDimensions when the window resizes after canvas init', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);
            component.onCanvasInitialized();
            canvas.getCanvasDimensions.mockReturnValue({ width: 1600, height: 900 });

            component.handleWindowResize();

            expect(component.canvasDimensions()).toEqual({ width: 1600, height: 900 });
        }));

        it('should not call into the canvas on resize before initialization', fakeAsync(() => {
            const { fixture, component } = setup();
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            canvas.getCanvasDimensions.mockClear();

            component.handleWindowResize();

            expect(canvas.getCanvasDimensions).not.toHaveBeenCalled();
        }));

        it('should pass birdseye signals through to connector-graph-controls', fakeAsync(() => {
            const { fixture, component } = setup({ parentProcessGroupId: 'parent-pg' });
            fixture.detectChanges();
            tick();

            const canvas = getCanvasMock(fixture);
            attachCanvasMock(component, canvas);
            const seeded = [
                {
                    id: 'p1',
                    type: ComponentType.Processor,
                    position: { x: 0, y: 0 },
                    dimensions: { width: 10, height: 10 }
                }
            ];
            canvas.getBirdseyeComponentData.mockReturnValue(seeded);
            canvas.getCanvasDimensions.mockReturnValue({ width: 800, height: 600 });
            component.onCanvasInitialized();
            component.onTransformChange({ translate: { x: -10, y: -20 }, scale: 0.75 });
            fixture.detectChanges();

            const graphControls = getGraphControlsMock(fixture);

            expect(graphControls.birdseyeComponents()).toEqual(seeded);
            expect(graphControls.birdseyeTransform()).toEqual({ translate: { x: -10, y: -20 }, scale: 0.75 });
            expect(graphControls.canvasDimensions()).toEqual({ width: 800, height: 600 });
            expect(graphControls.canNavigateToParent()).toBe(true);
        }));
    });
});
