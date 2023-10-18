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
import { Store } from '@ngrx/store';
import { CanvasState } from '../../state';
import {
    selectConnection,
    selectCurrentProcessGroupId,
    selectFunnel,
    selectInputPort,
    selectLabel,
    selectOutputPort,
    selectProcessGroup,
    selectProcessor,
    selectRemoteProcessGroup,
    selectSingleEditedComponent
} from '../../state/flow/flow.selectors';
import { filter, map, switchMap, take } from 'rxjs';
import { ComponentType } from '../../../../state/shared';
import { SelectedComponent } from '../../state/flow';
import { editComponent } from '../../state/flow/flow.actions';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'edit-canvas-item',
    template: ''
})
export class EditCanvasItem {
    constructor(private store: Store<CanvasState>) {
        this.store
            .select(selectCurrentProcessGroupId)
            .pipe(
                filter((processGroupId) => processGroupId != null),
                switchMap(() => this.store.select(selectSingleEditedComponent)),
                // ensure there is a selected component
                filter((selectedComponent) => selectedComponent != null),
                switchMap((selectedComponent) => {
                    // @ts-ignore
                    const component: SelectedComponent = selectedComponent;

                    let component$;
                    switch (component.componentType) {
                        case ComponentType.Processor:
                            component$ = this.store.select(selectProcessor(component.id));
                            break;
                        case ComponentType.InputPort:
                            component$ = this.store.select(selectInputPort(component.id));
                            break;
                        case ComponentType.OutputPort:
                            component$ = this.store.select(selectOutputPort(component.id));
                            break;
                        case ComponentType.ProcessGroup:
                            component$ = this.store.select(selectProcessGroup(component.id));
                            break;
                        case ComponentType.RemoteProcessGroup:
                            component$ = this.store.select(selectRemoteProcessGroup(component.id));
                            break;
                        case ComponentType.Connection:
                            component$ = this.store.select(selectConnection(component.id));
                            break;
                        case ComponentType.Funnel:
                            component$ = this.store.select(selectFunnel(component.id));
                            break;
                        case ComponentType.Label:
                            component$ = this.store.select(selectLabel(component.id));
                            break;
                        default:
                            throw 'Unrecognized Component Type';
                    }

                    // combine the original selection with the component
                    return component$.pipe(
                        take(1),
                        map((component) => [selectedComponent, component])
                    );
                }),
                takeUntilDestroyed()
            )
            .subscribe(([selectedComponent, component]) => {
                // initiate the edit request
                this.store.dispatch(
                    editComponent({
                        request: {
                            type: selectedComponent.componentType,
                            uri: component.uri,
                            entity: component
                        }
                    })
                );
            });
    }
}
