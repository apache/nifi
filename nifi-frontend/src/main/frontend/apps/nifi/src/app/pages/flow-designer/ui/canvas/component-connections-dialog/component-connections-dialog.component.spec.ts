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
import { provideMockStore, MockStore } from '@ngrx/store/testing';
import { of } from 'rxjs';
import { ComponentType } from '@nifi/shared';

import { ComponentConnectionsDialog } from './component-connections-dialog.component';
import { ComponentConnectionsDialogRequest, ConnectionDirection, ConnectionEntity } from '../../../state/flow';
import { navigateToComponent } from '../../../state/flow/flow.actions';
import { CanvasUtils } from '../../../service/canvas-utils.service';

const COMPONENT_ID = 'a1b2c3d4-0000-0000-0000-000000000000';
const GROUP_ID = 'e5f6a7b8-0000-0000-0000-000000000000';
const CHILD_GROUP_ID = 'c9d0e1f2-0000-0000-0000-000000000000';

interface ConnectableStub {
    id: string;
    name: string;
}

/**
 * Builds a readable connection between the two supplied endpoints. The endpoints are reported as
 * living in the group being viewed, which is the common case; {@link connectionIntoChildGroup}
 * covers an endpoint inside a child group.
 */
function connection(
    id: string,
    source: ConnectableStub,
    destination: ConnectableStub,
    component: { name?: string; selectedRelationships?: string[] } = {}
): ConnectionEntity {
    return {
        id,
        permissions: { canRead: true, canWrite: true },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId: source.id,
        sourceGroupId: GROUP_ID,
        sourceType: 'PROCESSOR',
        destinationId: destination.id,
        destinationGroupId: GROUP_ID,
        destinationType: 'INPUT_PORT',
        component: {
            id,
            source,
            destination,
            ...component
        }
    };
}

/**
 * Builds a connection the current user cannot read. The API omits the component entirely in that case
 * but still reports the endpoints at the top level of the entity.
 */
function unreadableConnection(id: string, sourceId: string, destinationId: string): ConnectionEntity {
    return {
        id,
        permissions: { canRead: false, canWrite: false },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId,
        sourceGroupId: GROUP_ID,
        sourceType: 'PROCESSOR',
        destinationId,
        destinationGroupId: GROUP_ID,
        destinationType: 'INPUT_PORT',
        component: null
    };
}

/**
 * Builds a connection from a component in the viewed group to an Input Port inside a child group.
 * The canvas draws this as terminating at the child group, but the entity names the port.
 */
function connectionIntoChildGroup(
    id: string,
    source: ConnectableStub,
    innerPort: ConnectableStub,
    name: string
): ConnectionEntity {
    return {
        id,
        permissions: { canRead: true, canWrite: true },
        position: { x: 0, y: 0 },
        revision: { version: 0 },
        sourceId: source.id,
        sourceGroupId: GROUP_ID,
        sourceType: 'PROCESSOR',
        destinationId: innerPort.id,
        destinationGroupId: CHILD_GROUP_ID,
        destinationType: 'INPUT_PORT',
        component: {
            id,
            name,
            source,
            destination: innerPort
        }
    };
}

interface CreatedDialog {
    component: ComponentConnectionsDialog;
    fixture: ComponentFixture<ComponentConnectionsDialog>;
    store: MockStore;
    dialogRef: { close: ReturnType<typeof vi.fn>; keydownEvents: () => ReturnType<typeof of> };
}

function createDialog(
    direction: ConnectionDirection,
    connections: ConnectionEntity[],
    componentName = 'In'
): CreatedDialog {
    const dialogRequest: ComponentConnectionsDialogRequest = {
        componentName,
        groupId: GROUP_ID,
        direction,
        connections
    };
    const dialogRef = { close: vi.fn(), keydownEvents: () => of() };

    // only formatConnectionName is exercised; the real CanvasUtils subscribes to canvas state on
    // construction, which this dialog has no need of
    const canvasUtils = {
        formatConnectionName: (component: any): string => {
            if (component.name) {
                return component.name;
            }
            if (component.selectedRelationships) {
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

    const fixture: ComponentFixture<ComponentConnectionsDialog> = TestBed.createComponent(ComponentConnectionsDialog);
    fixture.detectChanges();
    return {
        component: fixture.componentInstance,
        fixture,
        store: TestBed.inject(MockStore),
        dialogRef
    };
}

describe('ComponentConnectionsDialog', () => {
    it('should create', () => {
        const { component } = createDialog('upstream', []);
        expect(component).toBeTruthy();
    });

    describe('upstream', () => {
        it('renders a row per connection showing both endpoints and the connection name', () => {
            const { component, fixture } = createDialog('upstream', [
                connection(
                    'c1',
                    { id: 's1', name: 'GenerateFlowFile' },
                    { id: COMPONENT_ID, name: 'In' },
                    {
                        selectedRelationships: ['success']
                    }
                ),
                connection(
                    'c2',
                    { id: 's2', name: 'UpdateAttribute' },
                    { id: COMPONENT_ID, name: 'In' },
                    {
                        name: 'to the port'
                    }
                )
            ]);

            expect(component.title).toBe('Upstream Connections');
            expect(component.componentName).toBe('In');
            expect(component.rows).toEqual([
                {
                    id: 'c1',
                    name: 'success',
                    source: { name: 'GenerateFlowFile', id: 's1' },
                    destination: { name: 'In', id: COMPONENT_ID }
                },
                {
                    id: 'c2',
                    name: 'to the port',
                    source: { name: 'UpdateAttribute', id: 's2' },
                    destination: { name: 'In', id: COMPONENT_ID }
                }
            ]);

            const text = fixture.nativeElement.textContent;
            expect(text).toContain('GenerateFlowFile');
            expect(text).toContain('UpdateAttribute');
            expect(text).toContain('to the port');
        });

        it('keeps a connection the user cannot read and marks both endpoints Unauthorized', () => {
            const { component, fixture } = createDialog('upstream', [
                unreadableConnection('c1', 'hidden-source', COMPONENT_ID)
            ]);

            expect(component.rows).toEqual([
                {
                    id: 'c1',
                    name: null,
                    source: { name: null, id: 'hidden-source' },
                    destination: { name: null, id: COMPONENT_ID }
                }
            ]);
            expect(fixture.nativeElement.textContent).toContain('Unauthorized');
        });

        it('renders an Unnamed placeholder for a connection with neither a name nor relationships', () => {
            const { component, fixture } = createDialog('upstream', [
                connection('c1', { id: 's1', name: 'Other Port' }, { id: COMPONENT_ID, name: 'In' })
            ]);

            expect(component.rows[0].name).toBeNull();
            expect(fixture.nativeElement.textContent).toContain('Unnamed');
        });
    });

    describe('downstream', () => {
        it('reports the destination alongside the source', () => {
            const { component } = createDialog(
                'downstream',
                [
                    connection(
                        'c1',
                        { id: COMPONENT_ID, name: 'Out' },
                        { id: 'd1', name: 'LogAttribute' },
                        {
                            name: 'from the port'
                        }
                    )
                ],
                'Out'
            );

            expect(component.title).toBe('Downstream Connections');
            expect(component.rows).toEqual([
                {
                    id: 'c1',
                    name: 'from the port',
                    source: { name: 'Out', id: COMPONENT_ID },
                    destination: { name: 'LogAttribute', id: 'd1' }
                }
            ]);
        });

        it('names the port inside a child group rather than the group the canvas draws', () => {
            const { component, fixture } = createDialog(
                'downstream',
                [
                    connectionIntoChildGroup(
                        'c1',
                        { id: 's1', name: 'GenerateFlowFile' },
                        { id: 'inner-port', name: 'Inner In' },
                        'into the group'
                    )
                ],
                'Child Group'
            );

            expect(component.rows).toEqual([
                {
                    id: 'c1',
                    name: 'into the group',
                    source: { name: 'GenerateFlowFile', id: 's1' },
                    destination: { name: 'Inner In', id: 'inner-port' }
                }
            ]);
            expect(fixture.nativeElement.textContent).toContain('Inner In');
        });
    });

    describe('empty state', () => {
        it('reports that no upstream connections were found', () => {
            const { fixture } = createDialog('upstream', []);
            expect(fixture.nativeElement.textContent).toContain('No upstream connections were found.');
        });

        it('reports that no downstream connections were found', () => {
            const { fixture } = createDialog('downstream', []);
            expect(fixture.nativeElement.textContent).toContain('No downstream connections were found.');
        });
    });

    describe('navigation', () => {
        it('navigates to the connection in the group that defines it and closes the dialog', () => {
            const { component, store, dialogRef } = createDialog('upstream', [
                connection('c1', { id: 's1', name: 'GenerateFlowFile' }, { id: COMPONENT_ID, name: 'In' })
            ]);
            const dispatch = vi.spyOn(store, 'dispatch');

            component.goTo(component.rows[0]);

            expect(dispatch).toHaveBeenCalledWith(
                navigateToComponent({
                    request: {
                        id: 'c1',
                        processGroupId: GROUP_ID,
                        type: ComponentType.Connection
                    }
                })
            );
            expect(dialogRef.close).toHaveBeenCalled();
        });
    });
});
