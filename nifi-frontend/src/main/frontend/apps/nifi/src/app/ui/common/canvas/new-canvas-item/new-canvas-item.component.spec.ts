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
import { CdkDragEnd } from '@angular/cdk/drag-drop';

import { NewCanvasItem } from './new-canvas-item.component';

describe('NewCanvasItem', () => {
    let component: NewCanvasItem;
    let fixture: ComponentFixture<NewCanvasItem>;

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [NewCanvasItem]
        });
        fixture = TestBed.createComponent(NewCanvasItem);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });

    it('emits dragStarted when CDK drag begins', () => {
        const startedSpy = vi.fn();
        component.dragStarted.subscribe(startedSpy);

        component.onDragStarted();

        expect(startedSpy).toHaveBeenCalledTimes(1);
    });

    it('emits dragEnded with the original event and resets the drag ref', () => {
        const endedSpy = vi.fn();
        component.dragEnded.subscribe(endedSpy);

        const reset = vi.fn();
        const event = {
            source: { _dragRef: { reset } }
        } as unknown as CdkDragEnd;

        component.onDragEnded(event);

        expect(endedSpy).toHaveBeenCalledWith(event);
        expect(reset).toHaveBeenCalledTimes(1);
    });

    it('suppresses hover state while dragging is active', () => {
        component.dragging = true;
        component.mouseEnter();

        expect(component.isHovering()).toBe(false);

        component.dragging = false;
        expect(component.isHovering()).toBe(true);
    });

    it('ignores hover events when disabled', () => {
        component.disabled = true;
        component.mouseEnter();
        expect(component.isHovering()).toBe(false);
    });
});
