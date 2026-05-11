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
import { CommonModule } from '@angular/common';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { NoopAnimationsModule } from '@angular/platform-browser/animations';

import { ConnectorGraphControls } from './connector-graph-controls.component';
import { ConnectorInfoControl } from './connector-info-control/connector-info-control.component';
import { ConnectorEntity } from '@nifi/shared';
import { BirdseyeComponentData, BirdseyeTransform } from '../../../../../ui/common/birdseye/birdseye.types';
import { Dimension } from '../../../../../ui/common/canvas/canvas.types';

@Component({
    selector: 'connector-info-control',
    standalone: true,
    imports: [CommonModule],
    template: ''
})
class MockConnectorInfoControl {
    connectorEntity = input<ConnectorEntity | null>(null);
    entitySaving = input<boolean>(false);
}

interface SetupInputs {
    connectorEntity?: ConnectorEntity | null;
    entitySaving?: boolean;
    birdseyeComponents?: BirdseyeComponentData[];
    birdseyeTransform?: BirdseyeTransform;
    canvasDimensions?: Dimension;
}

async function setup(inputs: SetupInputs = {}) {
    await TestBed.configureTestingModule({
        imports: [ConnectorGraphControls, NoopAnimationsModule]
    })
        .overrideComponent(ConnectorGraphControls, {
            remove: { imports: [ConnectorInfoControl] },
            add: { imports: [MockConnectorInfoControl] }
        })
        .compileComponents();

    const fixture: ComponentFixture<ConnectorGraphControls> = TestBed.createComponent(ConnectorGraphControls);
    fixture.componentRef.setInput('connectorEntity', inputs.connectorEntity ?? null);
    fixture.componentRef.setInput('entitySaving', inputs.entitySaving ?? false);
    fixture.componentRef.setInput('birdseyeComponents', inputs.birdseyeComponents ?? []);
    fixture.componentRef.setInput(
        'birdseyeTransform',
        inputs.birdseyeTransform ?? { translate: { x: 0, y: 0 }, scale: 1 }
    );
    fixture.componentRef.setInput('canvasDimensions', inputs.canvasDimensions ?? { width: 0, height: 0 });
    fixture.detectChanges();

    return { fixture, component: fixture.componentInstance };
}

describe('ConnectorGraphControls', () => {
    it('should create', async () => {
        const { component } = await setup();
        expect(component).toBeTruthy();
    });

    it('should render the connector-info-control child component', async () => {
        const { fixture } = await setup();
        const infoControl = fixture.nativeElement.querySelector('connector-info-control');
        expect(infoControl).toBeTruthy();
    });
});
