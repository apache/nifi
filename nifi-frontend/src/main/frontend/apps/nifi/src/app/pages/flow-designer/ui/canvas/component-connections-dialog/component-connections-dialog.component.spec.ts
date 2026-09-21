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

import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { MockStore, provideMockStore } from '@ngrx/store/testing';
import { By } from '@angular/platform-browser';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';
import { ComponentType } from '@nifi/shared';

import { ComponentConnectionsDialog, ComponentConnectionRow } from './component-connections-dialog.component';
import { ComponentConnectionsDialogRequest, ConnectionDirection, ConnectionEntity } from '../../../state/flow';
import { enterProcessGroup, navigateToComponent } from '../../../state/flow/flow.actions';
import { CanvasUtils } from '../../../service/canvas-utils.service';

const REQUEST_GROUP_ID = 'request-group-id';
const SOURCE_GROUP_ID = 'source-group-id';
const DESTINATION_GROUP_ID = 'destination-group-id';
const UNKNOWN_GROUP_ID = 'unknown-group-id';
const REMOTE_GROUP_ID = 'remote-process-group-id';

// when the user selects nothing on the canvas the current group itself is reported, and its connections
// are defined in the parent group, so the parent is the group the dialog request is built around
const PARENT_GROUP_ID = 'parent-group-id';
const CURRENT_GROUP_ID = 'current-group-id';
const SIBLING_GROUP_ID = 'sibling-group-id';

const SELECTED_COMPONENT_ID = 'selected-component-id';
const SOURCE_ID = 'source-id';
const DESTINATION_ID = 'destination-id';
const CONNECTION_ID = 'connection-id';

interface ConnectableStub {
    id: string;
    name: string;
}

interface ConnectionOptions {
    id?: string;
    source?: ConnectableStub;
    destination?: ConnectableStub;
    sourceGroupId?: string;
    destinationGroupId?: string;
    sourceType?: string;
    destinationType?: string;
    canRead?: boolean;
    name?: string;
    selectedRelationships?: string[];
    component?: any | null;
}

interface CreatedDialog {
    component: ComponentConnectionsDialog;
    fixture: ComponentFixture<ComponentConnectionsDialog>;
    store: MockStore;
    dialogRef: {
        close: ReturnType<typeof vi.fn>;
        keydownEvents: () => ReturnType<typeof of>;
    };
}

function readableConnection(options: ConnectionOptions = {}): ConnectionEntity {
    const source = options.source ?? { id: SOURCE_ID, name: 'GenerateFlowFile' };
    const destination = options.destination ?? { id: DESTINATION_ID, name: 'LogAttribute' };

    return {
        id: options.id ?? CONNECTION_ID,
        permissions: { canRead: options.canRead ?? true, canWrite: true },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId: source.id,
        sourceGroupId: options.sourceGroupId ?? SOURCE_GROUP_ID,
        sourceType: options.sourceType ?? 'PROCESSOR',
        destinationId: destination.id,
        destinationGroupId: options.destinationGroupId ?? DESTINATION_GROUP_ID,
        destinationType: options.destinationType ?? 'INPUT_PORT',
        component:
            options.component === undefined
                ? {
                      id: options.id ?? CONNECTION_ID,
                      source,
                      destination,
                      name: options.name,
                      selectedRelationships: options.selectedRelationships
                  }
                : options.component
    };
}

function unreadableConnection(options: ConnectionOptions = {}): ConnectionEntity {
    return {
        id: options.id ?? CONNECTION_ID,
        permissions: { canRead: false, canWrite: false },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId: options.source?.id ?? SOURCE_ID,
        sourceGroupId: options.sourceGroupId ?? SOURCE_GROUP_ID,
        sourceType: options.sourceType ?? 'PROCESSOR',
        destinationId: options.destination?.id ?? DESTINATION_ID,
        destinationGroupId: options.destinationGroupId ?? DESTINATION_GROUP_ID,
        destinationType: options.destinationType ?? 'INPUT_PORT',
        component: null
    };
}

/**
 * Builds the dialog. The group on the canvas defaults to the group the connections belong to, which is
 * where every component except a port searched across its own group's boundary is reported from.
 */
function createDialog(
    direction: ConnectionDirection,
    connections: ConnectionEntity[],
    overrides: Partial<ComponentConnectionsDialogRequest> = {},
    canvasGroupId?: string
): CreatedDialog {
    const dialogRequest: ComponentConnectionsDialogRequest = {
        componentId: SELECTED_COMPONENT_ID,
        componentName: 'Selected Component',
        componentType: ComponentType.InputPort,
        groupId: REQUEST_GROUP_ID,
        direction,
        connections,
        groupIdToName: new Map([
            [REQUEST_GROUP_ID, 'Current Process Group'],
            [SOURCE_GROUP_ID, 'Source Process Group'],
            [DESTINATION_GROUP_ID, 'Destination Process Group']
        ]),
        componentIdToName: new Map(),
        ...overrides
    };

    const dialogRef = {
        close: vi.fn(),
        keydownEvents: () => of()
    };

    const canvasUtils = {
        formatConnectionName: (component: any): string => {
            if (component?.name) {
                return component.name;
            }
            if (component?.selectedRelationships) {
                return component.selectedRelationships.join(', ');
            }
            return '';
        },
        getProcessGroupId: (): string => canvasGroupId ?? dialogRequest.groupId
    };

    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        imports: [ComponentConnectionsDialog, NoopAnimationsModule],
        providers: [
            { provide: MAT_DIALOG_DATA, useValue: dialogRequest },
            { provide: MatDialogRef, useValue: dialogRef },
            { provide: CanvasUtils, useValue: canvasUtils },
            provideMockStore({})
        ]
    });

    const fixture = TestBed.createComponent(ComponentConnectionsDialog);
    fixture.detectChanges();

    return {
        component: fixture.componentInstance,
        fixture,
        store: TestBed.inject(MockStore),
        dialogRef
    };
}

/**
 * Builds the dialog as it is opened when the user selects nothing on the canvas: the current group is
 * the reported component, and the group of the request is its parent, where its connections are defined.
 */
function createCurrentProcessGroupDialog(
    direction: ConnectionDirection,
    connections: ConnectionEntity[],
    overrides: Partial<ComponentConnectionsDialogRequest> = {}
): CreatedDialog {
    return createDialog(
        direction,
        connections,
        {
            componentId: CURRENT_GROUP_ID,
            componentName: 'Current Process Group',
            componentType: ComponentType.ProcessGroup,
            groupId: PARENT_GROUP_ID,
            groupIdToName: new Map([
                [PARENT_GROUP_ID, 'Parent Process Group'],
                [CURRENT_GROUP_ID, 'Current Process Group'],
                [SIBLING_GROUP_ID, 'Sibling Process Group']
            ]),
            ...overrides
        },
        CURRENT_GROUP_ID
    );
}

function textContent(fixture: ComponentFixture<ComponentConnectionsDialog>): string {
    return (fixture.nativeElement.textContent as string).replace(/\s+/g, ' ').trim();
}

function getCells(fixture: ComponentFixture<ComponentConnectionsDialog>, columnClass: string): HTMLElement[] {
    return fixture.debugElement.queryAll(By.css(`td.${columnClass}`)).map((debugElement) => debugElement.nativeElement);
}

function clickCell(fixture: ComponentFixture<ComponentConnectionsDialog>, columnClass: string): void {
    const link = getCells(fixture, columnClass)[0].querySelector('a') as HTMLAnchorElement;
    link.click();
}

function clickHeader(fixture: ComponentFixture<ComponentConnectionsDialog>, columnClass: string): void {
    const header = fixture.debugElement.query(By.css(`th.${columnClass}`)).nativeElement as HTMLElement;
    const sortButton = header.querySelector('button');
    (sortButton ?? header).click();
    fixture.detectChanges();
}

function renderedIds(component: ComponentConnectionsDialog): string[] {
    return component.dataSource.data.map((row) => row.id);
}

/**
 * Rebuilds the route the navigateToComponent effect pushes for a dispatched navigation, so that a test
 * can assert on the url the click produces rather than only on the component type carried by the action.
 */
function navigationUrl(dispatch: ReturnType<typeof vi.spyOn>): string {
    const { request } = dispatch.mock.calls[0][0] as ReturnType<typeof navigateToComponent>;
    return ['/process-groups', request.processGroupId, request.type, request.id].join('/');
}

describe('ComponentConnectionsDialog', () => {
    it('creates the dialog', () => {
        const { component } = createDialog('upstream', []);

        expect(component).toBeTruthy();
    });

    describe('dialog metadata', () => {
        it('sets upstream title and empty message', () => {
            const { component, fixture } = createDialog('upstream', []);

            expect(component.title).toBe('Upstream Connections');
            expect(component.emptyMessage).toBe('No upstream connections were found.');
            expect(textContent(fixture)).toContain('Upstream Connections');
            expect(textContent(fixture)).toContain('No upstream connections were found.');
        });

        it('sets downstream title and empty message', () => {
            const { component, fixture } = createDialog('downstream', []);

            expect(component.title).toBe('Downstream Connections');
            expect(component.emptyMessage).toBe('No downstream connections were found.');
            expect(textContent(fixture)).toContain('Downstream Connections');
            expect(textContent(fixture)).toContain('No downstream connections were found.');
        });

        it('reports the selected component through the shared component context', () => {
            const { fixture } = createDialog('upstream', [], {
                componentId: 'input-port-a-id',
                componentName: 'Input Port A',
                componentType: ComponentType.InputPort
            });

            const componentContext = fixture.debugElement.query(By.css('component-context'));
            expect(componentContext).not.toBeNull();

            const contextText = (componentContext.nativeElement.textContent as string).replace(/\s+/g, ' ').trim();
            expect(contextText).toContain('Input Port A');
            // the type label and copyable id the widget renders on top of the name
            expect(contextText).toContain('Input Port');
            expect(contextText).toContain('input-port-a-id');
            expect(componentContext.query(By.css('.icon-port-in'))).not.toBeNull();
        });

        it('reports an unreadable component by the id used in place of its name', () => {
            const { fixture } = createDialog('upstream', [], {
                componentId: 'unreadable-component-id',
                componentName: 'unreadable-component-id',
                componentType: ComponentType.Processor
            });

            const componentContext = fixture.debugElement.query(By.css('component-context'));
            const contextText = (componentContext.nativeElement.textContent as string).replace(/\s+/g, ' ').trim();

            expect(contextText).toContain('unreadable-component-id');
            expect(contextText).toContain('Processor');
        });

        it('reports a remote process group with the remote group icon', () => {
            const { fixture } = createDialog('downstream', [], {
                componentId: REMOTE_GROUP_ID,
                componentName: 'Remote Process Group A',
                componentType: ComponentType.RemoteProcessGroup
            });

            const componentContext = fixture.debugElement.query(By.css('component-context'));

            expect(componentContext.query(By.css('.icon-group-remote'))).not.toBeNull();
            expect(componentContext.nativeElement.textContent).toContain('Remote Process Group A');
        });
    });

    describe('row construction', () => {
        it('builds a row for a readable connection with a connection name', () => {
            const connection = readableConnection({
                id: 'named-connection-id',
                name: 'Named Connection',
                source: { id: 'processor-id', name: 'GenerateFlowFile' },
                destination: { id: 'input-port-id', name: 'Input Port' }
            });

            const { component } = createDialog('upstream', [connection]);

            expect(component.rows).toEqual<ComponentConnectionRow[]>([
                {
                    id: 'named-connection-id',
                    name: 'Named Connection',
                    source: {
                        id: 'processor-id',
                        groupId: SOURCE_GROUP_ID,
                        type: ComponentType.Processor,
                        name: 'GenerateFlowFile'
                    },
                    destination: {
                        id: 'input-port-id',
                        groupId: DESTINATION_GROUP_ID,
                        type: ComponentType.InputPort,
                        name: 'Input Port'
                    }
                }
            ]);
        });

        it('uses selected relationships as the connection name when no explicit connection name is present', () => {
            const connection = readableConnection({
                selectedRelationships: ['success', 'retry']
            });

            const { component, fixture } = createDialog('upstream', [connection]);

            expect(component.rows[0].name).toBe('success, retry');
            expect(textContent(fixture)).toContain('success, retry');
        });

        it('uses a null connection name when the formatted name is empty', () => {
            const connection = readableConnection();

            const { component } = createDialog('upstream', [connection]);

            expect(component.rows[0].name).toBeNull();
        });

        it('keeps unreadable connections using top-level endpoint identifiers and null endpoint names', () => {
            const connection = unreadableConnection({
                id: 'unreadable-connection-id',
                source: { id: 'hidden-source-id', name: 'Hidden Source' },
                destination: { id: 'hidden-destination-id', name: 'Hidden Destination' }
            });

            const { component, fixture } = createDialog('upstream', [connection]);

            expect(component.rows).toEqual<ComponentConnectionRow[]>([
                {
                    id: 'unreadable-connection-id',
                    name: null,
                    source: {
                        id: 'hidden-source-id',
                        groupId: SOURCE_GROUP_ID,
                        type: ComponentType.Processor,
                        name: null
                    },
                    destination: {
                        id: 'hidden-destination-id',
                        groupId: DESTINATION_GROUP_ID,
                        type: ComponentType.InputPort,
                        name: null
                    }
                }
            ]);

            expect(textContent(fixture)).toContain('Unauthorized');
        });

        it('maps remote input and output port endpoint types to RemoteProcessGroup', () => {
            const remoteInputConnection = readableConnection({
                id: 'remote-input-connection-id',
                sourceType: 'REMOTE_INPUT_PORT',
                destinationType: 'REMOTE_OUTPUT_PORT'
            });

            const { component } = createDialog('downstream', [remoteInputConnection]);

            expect(component.rows[0].source.type).toBe(ComponentType.RemoteProcessGroup);
            expect(component.rows[0].destination.type).toBe(ComponentType.RemoteProcessGroup);
        });

        it('maps unknown endpoint types to Connector', () => {
            const unknownTypeConnection = readableConnection({
                sourceType: 'UNKNOWN_SOURCE_TYPE',
                destinationType: 'UNKNOWN_DESTINATION_TYPE'
            });

            const { component } = createDialog('downstream', [unknownTypeConnection]);

            expect(component.rows[0].source.type).toBe(ComponentType.Connector);
            expect(component.rows[0].destination.type).toBe(ComponentType.Connector);
        });
    });

    describe('rendering', () => {
        it('renders the expected table columns', () => {
            const { component, fixture } = createDialog('upstream', [readableConnection()]);

            expect(component.displayedColumns).toEqual([
                'sourceProcessGroup',
                'sourceComponent',
                'connection',
                'destinationProcessGroup',
                'destinationComponent'
            ]);

            const renderedText = textContent(fixture);
            expect(renderedText).toContain('Source Process Group');
            expect(renderedText).toContain('Source Component');
            expect(renderedText).toContain('Connection');
            expect(renderedText).toContain('Destination Process Group');
            expect(renderedText).toContain('Destination Component');
        });

        it('renders process group names resolved from the request map', () => {
            const { fixture } = createDialog('upstream', [readableConnection()]);

            expect(textContent(fixture)).toContain('Source Process Group');
            expect(textContent(fixture)).toContain('Destination Process Group');
        });

        it('renders unknown process group ids when no name is available', () => {
            const connection = readableConnection({
                sourceGroupId: UNKNOWN_GROUP_ID,
                destinationGroupId: UNKNOWN_GROUP_ID
            });

            const { fixture } = createDialog('upstream', [connection]);

            expect(textContent(fixture)).toContain(UNKNOWN_GROUP_ID);
        });

        it('renders component names and the formatted connection name', () => {
            const connection = readableConnection({
                name: 'Connection Name',
                source: { id: 'source-component-id', name: 'Source Component Name' },
                destination: { id: 'destination-component-id', name: 'Destination Component Name' }
            });

            const { fixture } = createDialog('upstream', [connection]);

            const renderedText = textContent(fixture);
            expect(renderedText).toContain('Source Component Name');
            expect(renderedText).toContain('Connection Name');
            expect(renderedText).toContain('Destination Component Name');
        });

        it('renders "Connection" for an unnamed connection', () => {
            const { component, fixture } = createDialog('upstream', [readableConnection()]);

            expect(component.rows[0].name).toBeNull();
            expect(textContent(fixture)).toContain('Connection');
        });

        it('marks the header as sticky and applies striped row classes', () => {
            const { fixture } = createDialog('upstream', [
                readableConnection({ id: 'connection-1' }),
                readableConnection({ id: 'connection-2' })
            ]);

            expect(fixture.debugElement.query(By.css('tr.mat-mdc-header-row'))).not.toBeNull();

            const rows = fixture.debugElement.queryAll(By.css('tr.mat-mdc-row'));
            expect(rows.length).toBe(2);
            expect(rows[0].nativeElement.classList.contains('even')).toBeTruthy();
            expect(rows[1].nativeElement.classList.contains('even')).toBeFalsy();
        });

        it('renders table cells using component-connection-cell wrappers for truncation styling', () => {
            const { fixture } = createDialog('upstream', [readableConnection({ name: 'Named Connection' })]);

            expect(fixture.debugElement.queryAll(By.css('.component-connection-cell')).length).toBeGreaterThan(0);
        });
    });

    describe('process group name resolution', () => {
        it('resolves process group names from the dialog request map', () => {
            const { component } = createDialog('upstream', []);

            expect(component.resolveGroupName(REQUEST_GROUP_ID)).toBe('Current Process Group');
            expect(component.resolveGroupName(SOURCE_GROUP_ID)).toBe('Source Process Group');
            expect(component.resolveGroupName(DESTINATION_GROUP_ID)).toBe('Destination Process Group');
        });

        it('falls back to the group id when no process group name is available', () => {
            const { component } = createDialog('upstream', []);

            expect(component.resolveGroupName(UNKNOWN_GROUP_ID)).toBe(UNKNOWN_GROUP_ID);
        });

        it('offers nowhere to go for the group that both defines the connections and is on the canvas', () => {
            const { component } = createDialog('upstream', []);

            expect(component.isNavigableProcessGroup(REQUEST_GROUP_ID)).toBeFalsy();
            expect(component.isNavigableProcessGroup(SOURCE_GROUP_ID)).toBeTruthy();
        });

        it('offers the group that defines the connections when it is not the group on the canvas', () => {
            // a port searched across its own group's boundary reports connections defined in the parent
            const { component } = createDialog('upstream', [], {}, SOURCE_GROUP_ID);

            expect(component.isNavigableProcessGroup(REQUEST_GROUP_ID)).toBeTruthy();
            expect(component.isNavigableProcessGroup(SOURCE_GROUP_ID)).toBeTruthy();
        });
    });

    describe('navigation', () => {
        it('dispatches navigation and closes the dialog when navigateTo is called', () => {
            const { component, store, dialogRef } = createDialog('upstream', []);
            const dispatch = vi.spyOn(store, 'dispatch');

            component.navigateTo('target-id', 'target-group-id', ComponentType.Processor);

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'target-id',
                        processGroupId: 'target-group-id',
                        type: ComponentType.Processor
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('renders the current source process group as non-clickable', () => {
            const connection = readableConnection({
                sourceGroupId: REQUEST_GROUP_ID,
                destinationGroupId: DESTINATION_GROUP_ID
            });

            const { fixture } = createDialog('upstream', [connection]);

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            expect(sourceProcessGroupCell.querySelector('span')).not.toBeNull();
            expect(sourceProcessGroupCell.querySelector('a')).toBeNull();
        });

        it('renders a non-current source process group as clickable and navigates to it', () => {
            const connection = readableConnection({
                sourceGroupId: SOURCE_GROUP_ID
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            const link = sourceProcessGroupCell.querySelector('a') as HTMLAnchorElement;
            link.click();

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: SOURCE_GROUP_ID,
                        processGroupId: REQUEST_GROUP_ID,
                        type: ComponentType.ProcessGroup
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('renders the current destination process group as non-clickable', () => {
            const connection = readableConnection({
                sourceGroupId: SOURCE_GROUP_ID,
                destinationGroupId: REQUEST_GROUP_ID
            });

            const { fixture } = createDialog('upstream', [connection]);

            const destinationProcessGroupCell = getCells(fixture, 'mat-column-destinationProcessGroup')[0];
            expect(destinationProcessGroupCell.querySelector('span')).not.toBeNull();
            expect(destinationProcessGroupCell.querySelector('a')).toBeNull();
        });

        it('navigates to the readable source component using the source component group id', () => {
            const connection = readableConnection({
                source: { id: 'source-component-id', name: 'Source Component' },
                sourceGroupId: SOURCE_GROUP_ID,
                sourceType: 'PROCESSOR'
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];
            const link = sourceComponentCell.querySelector('a') as HTMLAnchorElement;
            link.click();

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'source-component-id',
                        processGroupId: SOURCE_GROUP_ID,
                        type: ComponentType.Processor
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('navigates to the readable destination component using the destination component group id', () => {
            const connection = readableConnection({
                destination: { id: 'destination-component-id', name: 'Destination Component' },
                destinationGroupId: DESTINATION_GROUP_ID,
                destinationType: 'OUTPUT_PORT'
            });

            const { fixture, store, dialogRef } = createDialog('downstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];
            const link = destinationComponentCell.querySelector('a') as HTMLAnchorElement;
            link.click();

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'destination-component-id',
                        processGroupId: DESTINATION_GROUP_ID,
                        type: ComponentType.OutputPort
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('keeps an unreadable component clickable so it can be reached like it can on the canvas', () => {
            const { fixture, store, dialogRef } = createDialog('upstream', [unreadableConnection()]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];
            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];

            expect(sourceComponentCell.textContent).toContain('Unauthorized');
            expect(destinationComponentCell.textContent).toContain('Unauthorized');
            expect(sourceComponentCell.querySelector('a')).not.toBeNull();
            expect(destinationComponentCell.querySelector('a')).not.toBeNull();

            clickCell(fixture, 'mat-column-sourceComponent');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: SOURCE_ID,
                        processGroupId: SOURCE_GROUP_ID,
                        type: ComponentType.Processor
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('identifies an unreadable component by its id, since the placeholder identifies nothing', () => {
            const { component } = createDialog('upstream', [
                readableConnection({
                    source: { id: 'readable-source-id', name: 'Readable Source' },
                    destination: { id: 'hidden-destination-id', name: 'Hidden Destination' }
                }),
                unreadableConnection({ destination: { id: 'hidden-destination-id', name: 'Hidden Destination' } })
            ]);

            expect(component.componentTooltip(component.rows[0].source)).toBe('Readable Source');
            expect(component.componentTooltip(component.rows[1].destination)).toBe('hidden-destination-id');
        });

        it('renders a remote input port source component as non-clickable', () => {
            const connection = readableConnection({
                source: { id: 'remote-input-port-id', name: 'Remote Input Port' },
                sourceGroupId: 'remote-process-group-id',
                sourceType: 'REMOTE_INPUT_PORT',
                destination: { id: 'processor-id', name: 'Processor' },
                destinationType: 'PROCESSOR'
            });

            const { fixture, store, dialogRef } = createDialog('downstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];

            expect(sourceComponentCell.textContent).toContain('Remote Input Port');
            expect(sourceComponentCell.querySelector('span')).not.toBeNull();
            expect(sourceComponentCell.querySelector('a')).toBeNull();
            expect(dispatch).not.toHaveBeenCalled();
            expect(dialogRef.close).not.toHaveBeenCalled();
        });

        it('renders a remote output port source component as non-clickable', () => {
            const connection = readableConnection({
                source: { id: 'remote-output-port-id', name: 'Remote Output Port' },
                sourceGroupId: 'remote-process-group-id',
                sourceType: 'REMOTE_OUTPUT_PORT',
                destination: { id: 'processor-id', name: 'Processor' },
                destinationType: 'PROCESSOR'
            });

            const { fixture, store, dialogRef } = createDialog('downstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];

            expect(sourceComponentCell.textContent).toContain('Remote Output Port');
            expect(sourceComponentCell.querySelector('span')).not.toBeNull();
            expect(sourceComponentCell.querySelector('a')).toBeNull();
            expect(dispatch).not.toHaveBeenCalled();
            expect(dialogRef.close).not.toHaveBeenCalled();
        });

        it('renders a remote input port destination component as non-clickable', () => {
            const connection = readableConnection({
                source: { id: 'processor-id', name: 'Processor' },
                sourceType: 'PROCESSOR',
                destination: { id: 'remote-input-port-id', name: 'Remote Input Port' },
                destinationGroupId: 'remote-process-group-id',
                destinationType: 'REMOTE_INPUT_PORT'
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];

            expect(destinationComponentCell.textContent).toContain('Remote Input Port');
            expect(destinationComponentCell.querySelector('span')).not.toBeNull();
            expect(destinationComponentCell.querySelector('a')).toBeNull();
            expect(dispatch).not.toHaveBeenCalled();
            expect(dialogRef.close).not.toHaveBeenCalled();
        });

        it('renders a remote output port destination component as non-clickable', () => {
            const connection = readableConnection({
                source: { id: 'processor-id', name: 'Processor' },
                sourceType: 'PROCESSOR',
                destination: { id: 'remote-output-port-id', name: 'Remote Output Port' },
                destinationGroupId: 'remote-process-group-id',
                destinationType: 'REMOTE_OUTPUT_PORT'
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];

            expect(destinationComponentCell.textContent).toContain('Remote Output Port');
            expect(destinationComponentCell.querySelector('span')).not.toBeNull();
            expect(destinationComponentCell.querySelector('a')).toBeNull();
            expect(dispatch).not.toHaveBeenCalled();
            expect(dialogRef.close).not.toHaveBeenCalled();
        });

        it('continues rendering standard input and output port components as clickable', () => {
            const connection = readableConnection({
                source: { id: 'output-port-id', name: 'Output Port' },
                sourceGroupId: SOURCE_GROUP_ID,
                sourceType: 'OUTPUT_PORT',
                destination: { id: 'input-port-id', name: 'Input Port' },
                destinationGroupId: DESTINATION_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });

            const { fixture } = createDialog('downstream', [connection]);

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];
            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];

            expect(sourceComponentCell.querySelector('a')).not.toBeNull();
            expect(sourceComponentCell.querySelector('span')).toBeNull();
            expect(destinationComponentCell.querySelector('a')).not.toBeNull();
            expect(destinationComponentCell.querySelector('span')).toBeNull();
        });

        it('navigates to the connection in the group that defines the dialog request', () => {
            const connection = readableConnection({
                id: 'connection-to-navigate-to',
                name: 'Connection To Navigate To'
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const connectionCell = getCells(fixture, 'mat-column-connection')[0];
            const link = connectionCell.querySelector('a') as HTMLAnchorElement;
            link.click();

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'connection-to-navigate-to',
                        processGroupId: REQUEST_GROUP_ID,
                        type: ComponentType.Connection
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });
    });

    /**
     * A connection can be read only when the current user can read both of its ends, so the names it
     * carries vanish as soon as either end is unreadable. Each end is therefore reported from what is
     * known about that component on its own.
     */
    describe('per-endpoint authorization', () => {
        const CHILD_GROUP_ID = 'child-group-id';
        const SELECTED_PORT_NAME = 'Input Port A';

        // an Input Port searched upstream: the connections are defined in the parent, the port is not
        // among the components of the parent, and the source processor there cannot be read
        function unreadableSourceIntoSelectedPort(): ConnectionEntity {
            return unreadableConnection({
                id: 'unreadable-connection-id',
                source: { id: 'hidden-processor-id', name: 'Hidden Processor' },
                sourceGroupId: REQUEST_GROUP_ID,
                sourceType: 'PROCESSOR',
                destination: { id: SELECTED_COMPONENT_ID, name: SELECTED_PORT_NAME },
                destinationGroupId: CHILD_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });
        }

        /**
         * The components of the child group holding the port are listed alongside those of the parent
         * group that defines the connections, each with its own read permission, which is how the port
         * is named while the processor at the other end is not.
         */
        function createPortDialog(
            connections: ConnectionEntity[],
            readableComponents: [string, string][] = [[SELECTED_COMPONENT_ID, SELECTED_PORT_NAME]]
        ): CreatedDialog {
            return createDialog(
                'upstream',
                connections,
                {
                    componentId: SELECTED_COMPONENT_ID,
                    componentName: SELECTED_PORT_NAME,
                    componentType: ComponentType.InputPort,
                    componentIdToName: new Map(readableComponents)
                },
                CHILD_GROUP_ID
            );
        }

        it('names the readable end of a connection the user cannot read', () => {
            const { component, fixture } = createPortDialog([unreadableSourceIntoSelectedPort()]);

            expect(component.rows[0].source.name).toBeNull();
            expect(component.rows[0].destination.name).toBe(SELECTED_PORT_NAME);

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];
            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];
            expect(sourceComponentCell.textContent).toContain('Unauthorized');
            expect(destinationComponentCell.textContent).toContain(SELECTED_PORT_NAME);
            expect(destinationComponentCell.textContent).not.toContain('Unauthorized');
        });

        it('navigates to the readable end of a connection the user cannot read', () => {
            const { fixture, store, dialogRef } = createPortDialog([unreadableSourceIntoSelectedPort()]);
            const dispatch = vi.spyOn(store, 'dispatch');

            clickCell(fixture, 'mat-column-destinationComponent');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: SELECTED_COMPONENT_ID,
                        processGroupId: CHILD_GROUP_ID,
                        type: ComponentType.InputPort
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('reports an end that no group reported as unauthorized, whichever end it is', () => {
            // the component the connections were requested for is reported by its own group like any
            // other component, and is unauthorized when that group did not report it
            const { component } = createPortDialog([unreadableSourceIntoSelectedPort()], []);

            expect(component.rows[0].source.name).toBeNull();
            expect(component.rows[0].destination.name).toBeNull();
            expect(component.formatComponentName(component.rows[0].destination)).toBe('Unauthorized');
        });

        it('names both ends when each of their groups reported them', () => {
            const { component } = createPortDialog(
                [unreadableSourceIntoSelectedPort()],
                [
                    ['hidden-processor-id', 'No Longer Hidden Processor'],
                    [SELECTED_COMPONENT_ID, SELECTED_PORT_NAME]
                ]
            );

            expect(component.rows[0].source.name).toBe('No Longer Hidden Processor');
            expect(component.rows[0].destination.name).toBe(SELECTED_PORT_NAME);
        });

        it('names a funnel by its type, which is all the canvas shows for one', () => {
            const funnelConnection = unreadableConnection({
                source: { id: 'funnel-id', name: '' },
                sourceGroupId: REQUEST_GROUP_ID,
                sourceType: 'FUNNEL',
                destination: { id: SELECTED_COMPONENT_ID, name: SELECTED_PORT_NAME },
                destinationGroupId: CHILD_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });

            const { component } = createPortDialog([funnelConnection]);

            expect(component.rows[0].source.name).toBe('Funnel');
            expect(component.formatComponentName(component.rows[0].source)).toBe('Funnel');
        });

        it('falls back to the name the connection carries for an end no group reports', () => {
            // a port inside a Remote Process Group is listed by no flow of its own, and a readable
            // connection - which means both of its ends are readable - is what names it
            const connection = readableConnection({
                source: { id: 'remote-output-port-id', name: 'Remote Output Port' },
                sourceGroupId: REMOTE_GROUP_ID,
                sourceType: 'REMOTE_OUTPUT_PORT',
                destination: { id: SELECTED_COMPONENT_ID, name: SELECTED_PORT_NAME },
                destinationGroupId: CHILD_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });

            const { component } = createPortDialog([connection], []);

            expect(component.rows[0].source.name).toBe('Remote Output Port');
            expect(component.rows[0].destination.name).toBe(SELECTED_PORT_NAME);
        });
    });

    /**
     * A remote port's group is a Remote Process Group, which is navigated to as a component of the group
     * on the canvas rather than as a group that can be entered. The type carried by the navigation is the
     * ':type' segment of the resulting route, so a Process Group type here produces the wrong url.
     */
    describe('remote process group navigation', () => {
        it('navigates to a remote source process group as a Remote Process Group', () => {
            const connection = readableConnection({
                source: { id: 'remote-output-port-id', name: 'Remote Output Port' },
                sourceGroupId: REMOTE_GROUP_ID,
                sourceType: 'REMOTE_OUTPUT_PORT',
                destination: { id: 'processor-id', name: 'Processor' },
                destinationGroupId: DESTINATION_GROUP_ID,
                destinationType: 'PROCESSOR'
            });

            const { fixture, store, dialogRef } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            expect(sourceProcessGroupCell.querySelector('i.icon-group-remote')).not.toBeNull();

            clickCell(fixture, 'mat-column-sourceProcessGroup');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: REMOTE_GROUP_ID,
                        processGroupId: REQUEST_GROUP_ID,
                        type: ComponentType.RemoteProcessGroup
                    }
                })
            );
            expect(navigationUrl(dispatch)).toBe(
                `/process-groups/${REQUEST_GROUP_ID}/${ComponentType.RemoteProcessGroup}/${REMOTE_GROUP_ID}`
            );
            expect(navigationUrl(dispatch)).not.toContain(`/${ComponentType.ProcessGroup}/`);
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('navigates to a remote destination process group as a Remote Process Group', () => {
            const connection = readableConnection({
                source: { id: 'processor-id', name: 'Processor' },
                sourceGroupId: SOURCE_GROUP_ID,
                sourceType: 'PROCESSOR',
                destination: { id: 'remote-input-port-id', name: 'Remote Input Port' },
                destinationGroupId: REMOTE_GROUP_ID,
                destinationType: 'REMOTE_INPUT_PORT'
            });

            const { fixture, store, dialogRef } = createDialog('downstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const destinationProcessGroupCell = getCells(fixture, 'mat-column-destinationProcessGroup')[0];
            expect(destinationProcessGroupCell.querySelector('i.icon-group-remote')).not.toBeNull();

            clickCell(fixture, 'mat-column-destinationProcessGroup');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: REMOTE_GROUP_ID,
                        processGroupId: REQUEST_GROUP_ID,
                        type: ComponentType.RemoteProcessGroup
                    }
                })
            );
            expect(navigationUrl(dispatch)).toBe(
                `/process-groups/${REQUEST_GROUP_ID}/${ComponentType.RemoteProcessGroup}/${REMOTE_GROUP_ID}`
            );
            expect(navigationUrl(dispatch)).not.toContain(`/${ComponentType.ProcessGroup}/`);
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('keeps navigating to a local process group as a Process Group', () => {
            const connection = readableConnection({
                source: { id: 'output-port-id', name: 'Output Port' },
                sourceGroupId: SOURCE_GROUP_ID,
                sourceType: 'OUTPUT_PORT'
            });

            const { fixture, store } = createDialog('upstream', [connection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            expect(sourceProcessGroupCell.querySelector('i.icon-group')).not.toBeNull();

            clickCell(fixture, 'mat-column-sourceProcessGroup');

            expect(navigationUrl(dispatch)).toBe(
                `/process-groups/${REQUEST_GROUP_ID}/${ComponentType.ProcessGroup}/${SOURCE_GROUP_ID}`
            );
        });
    });

    /**
     * Right-clicking empty canvas selects no component, which implicitly reports the current process
     * group. Its connections are defined one level up, so the dialog is built around the parent group:
     * the parent is the group treated as current by the table, and the group the user is in is itself a
     * navigable component within it.
     */
    describe('current process group selection', () => {
        function upstreamIntoCurrentGroup(): ConnectionEntity {
            return readableConnection({
                source: { id: 'parent-processor-id', name: 'Parent Processor' },
                sourceGroupId: PARENT_GROUP_ID,
                sourceType: 'PROCESSOR',
                destination: { id: 'input-port-id', name: 'Input Port' },
                destinationGroupId: CURRENT_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });
        }

        function downstreamOutOfCurrentGroup(): ConnectionEntity {
            return readableConnection({
                source: { id: 'output-port-id', name: 'Output Port' },
                sourceGroupId: CURRENT_GROUP_ID,
                sourceType: 'OUTPUT_PORT',
                destination: { id: 'parent-processor-id', name: 'Parent Processor' },
                destinationGroupId: PARENT_GROUP_ID,
                destinationType: 'PROCESSOR'
            });
        }

        it('reports the current process group as the selected component', () => {
            const { component, fixture } = createCurrentProcessGroupDialog('upstream', [upstreamIntoCurrentGroup()]);

            expect(component.componentType).toBe(ComponentType.ProcessGroup);
            expect(component.componentId).toBe(CURRENT_GROUP_ID);

            const componentContext = fixture.debugElement.query(By.css('component-context'));
            const contextText = (componentContext.nativeElement.textContent as string).replace(/\s+/g, ' ').trim();

            expect(contextText).toContain('Current Process Group');
            expect(contextText).toContain('Process Group');
            expect(contextText).toContain(CURRENT_GROUP_ID);
            expect(componentContext.query(By.css('.icon-group'))).not.toBeNull();
        });

        it('offers both the parent group and the group on the canvas as places to go', () => {
            const { component } = createCurrentProcessGroupDialog('upstream', [upstreamIntoCurrentGroup()]);

            expect(component.isNavigableProcessGroup(PARENT_GROUP_ID)).toBeTruthy();
            expect(component.isNavigableProcessGroup(CURRENT_GROUP_ID)).toBeTruthy();
        });

        it('enters the parent group from the cell of a component that sits in it', () => {
            const { fixture, store, dialogRef } = createCurrentProcessGroupDialog('upstream', [
                upstreamIntoCurrentGroup()
            ]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            expect(sourceProcessGroupCell.textContent).toContain('Parent Process Group');

            clickCell(fixture, 'mat-column-sourceProcessGroup');

            // the parent holds no component of its own to select, so it is entered instead
            expect(dispatch).toHaveBeenCalledWith(enterProcessGroup({ request: { id: PARENT_GROUP_ID } }));
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('navigates into the current process group from the upstream destination group cell', () => {
            const { fixture, store, dialogRef } = createCurrentProcessGroupDialog('upstream', [
                upstreamIntoCurrentGroup()
            ]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const destinationProcessGroupCell = getCells(fixture, 'mat-column-destinationProcessGroup')[0];
            expect(destinationProcessGroupCell.textContent).toContain('Current Process Group');

            clickCell(fixture, 'mat-column-destinationProcessGroup');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: CURRENT_GROUP_ID,
                        processGroupId: PARENT_GROUP_ID,
                        type: ComponentType.ProcessGroup
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('navigates into the current process group from the downstream source group cell', () => {
            const { fixture, store, dialogRef } = createCurrentProcessGroupDialog('downstream', [
                downstreamOutOfCurrentGroup()
            ]);
            const dispatch = vi.spyOn(store, 'dispatch');

            clickCell(fixture, 'mat-column-sourceProcessGroup');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: CURRENT_GROUP_ID,
                        processGroupId: PARENT_GROUP_ID,
                        type: ComponentType.ProcessGroup
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });

        it('navigates to the port of a sibling group feeding the current process group', () => {
            const siblingConnection = readableConnection({
                source: { id: 'sibling-output-port-id', name: 'Sibling Output Port' },
                sourceGroupId: SIBLING_GROUP_ID,
                sourceType: 'OUTPUT_PORT',
                destination: { id: 'input-port-id', name: 'Input Port' },
                destinationGroupId: CURRENT_GROUP_ID,
                destinationType: 'INPUT_PORT'
            });

            const { fixture, store } = createCurrentProcessGroupDialog('upstream', [siblingConnection]);
            const dispatch = vi.spyOn(store, 'dispatch');

            const sourceProcessGroupCell = getCells(fixture, 'mat-column-sourceProcessGroup')[0];
            expect(sourceProcessGroupCell.textContent).toContain('Sibling Process Group');

            clickCell(fixture, 'mat-column-sourceComponent');

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'sibling-output-port-id',
                        processGroupId: SIBLING_GROUP_ID,
                        type: ComponentType.OutputPort
                    }
                })
            );
        });

        it('reports that the current process group has no connections in the requested direction', () => {
            const { fixture } = createCurrentProcessGroupDialog('upstream', []);

            const renderedText = textContent(fixture);
            expect(renderedText).toContain('Current Process Group');
            expect(renderedText).toContain('No upstream connections were found.');
        });
    });

    /**
     * Every column sorts on the text it renders, so a row is ordered by what the user reads in that
     * column rather than by the identifier behind it.
     */
    describe('sorting', () => {
        // resolved labels, by column: source group / source component / connection / destination group /
        // destination component
        const CURRENT_ROW_ID = 'current-source-group-connection-id'; // Current / Zeta / Beta / Destination / Alpha
        const SOURCE_ROW_ID = 'source-source-group-connection-id'; // Source / Alpha / Alpha / Current / Zeta
        const DESTINATION_ROW_ID = 'destination-source-group-connection-id'; // Destination / Gamma / Gamma / Source / Gamma

        function connectionsToSort(): ConnectionEntity[] {
            return [
                readableConnection({
                    id: CURRENT_ROW_ID,
                    name: 'Beta Connection',
                    source: { id: 'zeta-processor-id', name: 'Zeta Processor' },
                    sourceGroupId: REQUEST_GROUP_ID,
                    destination: { id: 'alpha-port-id', name: 'Alpha Port' },
                    destinationGroupId: DESTINATION_GROUP_ID
                }),
                readableConnection({
                    id: SOURCE_ROW_ID,
                    name: 'Alpha Connection',
                    source: { id: 'alpha-processor-id', name: 'Alpha Processor' },
                    sourceGroupId: SOURCE_GROUP_ID,
                    destination: { id: 'zeta-port-id', name: 'Zeta Port' },
                    destinationGroupId: REQUEST_GROUP_ID
                }),
                readableConnection({
                    id: DESTINATION_ROW_ID,
                    name: 'Gamma Connection',
                    source: { id: 'gamma-processor-id', name: 'Gamma Processor' },
                    sourceGroupId: DESTINATION_GROUP_ID,
                    destination: { id: 'gamma-port-id', name: 'Gamma Port' },
                    destinationGroupId: SOURCE_GROUP_ID
                })
            ];
        }

        it('sorts by the connection column ascending by default', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            expect(component.initialSortColumn).toBe('connection');
            expect(component.initialSortDirection).toBe('asc');
            expect(component.activeSort).toEqual({ active: 'connection', direction: 'asc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, CURRENT_ROW_ID, DESTINATION_ROW_ID]);
        });

        it('leaves the rows built from the request in their original order', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            expect(component.rows.map((row) => row.id)).toEqual([CURRENT_ROW_ID, SOURCE_ROW_ID, DESTINATION_ROW_ID]);
        });

        it('renders every column as sortable', () => {
            const { component, fixture } = createDialog('upstream', connectionsToSort());

            const sortableHeaders = fixture.debugElement.queryAll(By.css('th.mat-sort-header'));
            expect(sortableHeaders.length).toBe(component.displayedColumns.length);
        });

        it('sorts by the source process group name', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            component.sortData({ active: 'sourceProcessGroup', direction: 'asc' });
            expect(renderedIds(component)).toEqual([CURRENT_ROW_ID, DESTINATION_ROW_ID, SOURCE_ROW_ID]);

            component.sortData({ active: 'sourceProcessGroup', direction: 'desc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, DESTINATION_ROW_ID, CURRENT_ROW_ID]);
        });

        it('sorts by the source component name', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            component.sortData({ active: 'sourceComponent', direction: 'asc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, DESTINATION_ROW_ID, CURRENT_ROW_ID]);

            component.sortData({ active: 'sourceComponent', direction: 'desc' });
            expect(renderedIds(component)).toEqual([CURRENT_ROW_ID, DESTINATION_ROW_ID, SOURCE_ROW_ID]);
        });

        it('sorts by the connection name', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            component.sortData({ active: 'connection', direction: 'desc' });
            expect(renderedIds(component)).toEqual([DESTINATION_ROW_ID, CURRENT_ROW_ID, SOURCE_ROW_ID]);

            component.sortData({ active: 'connection', direction: 'asc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, CURRENT_ROW_ID, DESTINATION_ROW_ID]);
        });

        it('sorts by the destination process group name', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            component.sortData({ active: 'destinationProcessGroup', direction: 'asc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, CURRENT_ROW_ID, DESTINATION_ROW_ID]);

            component.sortData({ active: 'destinationProcessGroup', direction: 'desc' });
            expect(renderedIds(component)).toEqual([DESTINATION_ROW_ID, CURRENT_ROW_ID, SOURCE_ROW_ID]);
        });

        it('sorts by the destination component name', () => {
            const { component } = createDialog('upstream', connectionsToSort());

            component.sortData({ active: 'destinationComponent', direction: 'asc' });
            expect(renderedIds(component)).toEqual([CURRENT_ROW_ID, DESTINATION_ROW_ID, SOURCE_ROW_ID]);

            component.sortData({ active: 'destinationComponent', direction: 'desc' });
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, DESTINATION_ROW_ID, CURRENT_ROW_ID]);
        });

        it('sorts a group with no resolved name by the id it renders', () => {
            const connections = [
                readableConnection({ id: 'named-group-connection-id', sourceGroupId: SOURCE_GROUP_ID }),
                readableConnection({ id: 'unnamed-group-connection-id', sourceGroupId: UNKNOWN_GROUP_ID })
            ];

            const { component } = createDialog('upstream', connections);

            // 'Source Process Group' sorts ahead of the raw 'unknown-group-id' shown in place of a name
            component.sortData({ active: 'sourceProcessGroup', direction: 'asc' });
            expect(renderedIds(component)).toEqual(['named-group-connection-id', 'unnamed-group-connection-id']);
        });

        it('sorts unreadable components under the placeholder rendered for them', () => {
            const connections = [
                readableConnection({
                    id: 'zeta-connection-id',
                    source: { id: 'zeta-processor-id', name: 'Zeta Processor' }
                }),
                unreadableConnection({ id: 'first-unreadable-connection-id' }),
                readableConnection({
                    id: 'alpha-connection-id',
                    source: { id: 'alpha-processor-id', name: 'Alpha Processor' }
                }),
                unreadableConnection({ id: 'second-unreadable-connection-id' })
            ];

            const { component } = createDialog('upstream', connections);

            // 'Alpha Processor' < 'Unauthorized' < 'Zeta Processor', and rows sharing the placeholder keep
            // the order they were listed in
            component.sortData({ active: 'sourceComponent', direction: 'asc' });
            expect(renderedIds(component)).toEqual([
                'alpha-connection-id',
                'first-unreadable-connection-id',
                'second-unreadable-connection-id',
                'zeta-connection-id'
            ]);
        });

        it('sorts unnamed connections under the placeholder rendered for them', () => {
            const connections = [
                readableConnection({ id: 'delta-connection-id', name: 'Delta Connection' }),
                readableConnection({ id: 'unnamed-connection-id' }),
                readableConnection({ id: 'alpha-connection-id', name: 'Alpha Connection' })
            ];

            const { component } = createDialog('upstream', connections);

            // an unnamed connection renders 'Connection', which sorts between 'Alpha' and 'Delta'
            expect(component.rows[1].name).toBeNull();
            component.sortData({ active: 'connection', direction: 'asc' });
            expect(renderedIds(component)).toEqual([
                'alpha-connection-id',
                'unnamed-connection-id',
                'delta-connection-id'
            ]);
        });

        it('leaves the order unchanged for a column it does not sort on', () => {
            const { component } = createDialog('upstream', connectionsToSort());
            const orderBeforeSort = renderedIds(component);

            component.sortData({ active: 'unsortable-column', direction: 'asc' });

            expect(renderedIds(component)).toEqual(orderBeforeSort);
        });

        it('re-sorts the table when a column header is clicked', () => {
            const { component, fixture } = createDialog('upstream', connectionsToSort());

            clickHeader(fixture, 'mat-column-sourceComponent');

            expect(component.activeSort.active).toBe('sourceComponent');
            expect(component.activeSort.direction).toBe('asc');
            expect(renderedIds(component)).toEqual([SOURCE_ROW_ID, DESTINATION_ROW_ID, CURRENT_ROW_ID]);

            const sourceComponentCells = getCells(fixture, 'mat-column-sourceComponent');
            expect(sourceComponentCells[0].textContent).toContain('Alpha Processor');
            expect(sourceComponentCells[2].textContent).toContain('Zeta Processor');
        });

        it('reverses the order when the active column header is clicked again', () => {
            const { component, fixture } = createDialog('upstream', connectionsToSort());

            clickHeader(fixture, 'mat-column-connection');

            expect(component.activeSort).toEqual({ active: 'connection', direction: 'desc' });
            expect(renderedIds(component)).toEqual([DESTINATION_ROW_ID, CURRENT_ROW_ID, SOURCE_ROW_ID]);
        });
    });

    describe('icons', () => {
        it('returns the expected icon class for supported component types', () => {
            const { component } = createDialog('upstream', []);

            expect(component.componentIcon(ComponentType.Processor)).toBe('icon-processor');
            expect(component.componentIcon(ComponentType.InputPort)).toBe('icon-port-in');
            expect(component.componentIcon(ComponentType.OutputPort)).toBe('icon-port-out');
            expect(component.componentIcon(ComponentType.Funnel)).toBe('icon-funnel');
            expect(component.componentIcon(ComponentType.ProcessGroup)).toBe('icon-group');
            expect(component.componentIcon(ComponentType.RemoteProcessGroup)).toBe('icon-group-remote');
            expect(component.componentIcon(ComponentType.Connection)).toBe('icon-connect');
        });

        it('returns the drop icon for unsupported component types', () => {
            const { component } = createDialog('upstream', []);

            expect(component.componentIcon(ComponentType.ControllerService)).toBe('icon-drop');
        });
    });
});
