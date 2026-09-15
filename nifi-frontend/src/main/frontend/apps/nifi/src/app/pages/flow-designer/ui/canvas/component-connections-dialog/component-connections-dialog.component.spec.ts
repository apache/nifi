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
import { of } from 'rxjs';
import { ComponentType } from '@nifi/shared';

import { ComponentConnectionsDialog, ComponentConnectionRow } from './component-connections-dialog.component';
import { ComponentConnectionsDialogRequest, ConnectionDirection, ConnectionEntity } from '../../../state/flow';
import { navigateToComponent } from '../../../state/flow/flow.actions';
import { CanvasUtils } from '../../../service/canvas-utils.service';

const REQUEST_GROUP_ID = 'request-group-id';
const SOURCE_GROUP_ID = 'source-group-id';
const DESTINATION_GROUP_ID = 'destination-group-id';
const UNKNOWN_GROUP_ID = 'unknown-group-id';

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

function createDialog(
    direction: ConnectionDirection,
    connections: ConnectionEntity[],
    overrides: Partial<ComponentConnectionsDialogRequest> = {}
): CreatedDialog {
    const dialogRequest: ComponentConnectionsDialogRequest = {
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
        }
    };

    TestBed.resetTestingModule();
    TestBed.configureTestingModule({
        imports: [ComponentConnectionsDialog],
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

function textContent(fixture: ComponentFixture<ComponentConnectionsDialog>): string {
    return (fixture.nativeElement.textContent as string).replace(/\s+/g, ' ').trim();
}

function getCells(fixture: ComponentFixture<ComponentConnectionsDialog>, columnClass: string): HTMLElement[] {
    return fixture.debugElement.queryAll(By.css(`td.${columnClass}`)).map((debugElement) => debugElement.nativeElement);
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

        it('renders the selected component name and icon', () => {
            const { fixture } = createDialog('upstream', [], {
                componentName: 'Input Port A',
                componentType: ComponentType.InputPort
            });

            expect(textContent(fixture)).toContain('Selected Component');
            expect(textContent(fixture)).toContain('Input Port A');
            expect(fixture.debugElement.query(By.css('.icon-port-in'))).not.toBeNull();
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

        it('identifies the current process group from the dialog request group id', () => {
            const { component } = createDialog('upstream', []);

            expect(component.isCurrentProcessGroup(REQUEST_GROUP_ID)).toBeTruthy();
            expect(component.isCurrentProcessGroup(SOURCE_GROUP_ID)).toBeFalsy();
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

        it('does not render unreadable components as clickable', () => {
            const { fixture } = createDialog('upstream', [unreadableConnection()]);

            const sourceComponentCell = getCells(fixture, 'mat-column-sourceComponent')[0];
            const destinationComponentCell = getCells(fixture, 'mat-column-destinationComponent')[0];

            expect(sourceComponentCell.querySelector('a')).toBeNull();
            expect(destinationComponentCell.querySelector('a')).toBeNull();
            expect(sourceComponentCell.textContent).toContain('Unauthorized');
            expect(destinationComponentCell.textContent).toContain('Unauthorized');
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
