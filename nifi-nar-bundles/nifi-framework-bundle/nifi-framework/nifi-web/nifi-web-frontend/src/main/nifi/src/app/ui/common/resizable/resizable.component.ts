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

import { Component, ElementRef, EventEmitter, Output } from '@angular/core';
import { MatDialogModule } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { CdkDrag, CdkDragEnd, CdkDragMove } from '@angular/cdk/drag-drop';
import { auditTime, merge, of, Subject, tap, withLatestFrom } from 'rxjs';
import { AsyncPipe, NgIf } from '@angular/common';

@Component({
    selector: '[resizable]',
    standalone: true,
    imports: [MatDialogModule, MatButtonModule, CdkDrag, AsyncPipe, NgIf],
    templateUrl: './resizable.component.html',
    styleUrls: ['./resizable.component.scss']
})
export class Resizable {
    @Output() resized = new EventEmitter<DOMRect>();

    private startSize$ = new Subject<DOMRect>();
    private dragMove$ = new Subject<CdkDragMove>();
    private dragMoveAudited$ = this.dragMove$.pipe(
        withLatestFrom(this.startSize$),
        auditTime(25),
        tap(([{ distance }, rect]) => {
            this.el.nativeElement.style.width = `${rect.width + distance.x}px`;
            this.el.nativeElement.style.height = `${rect.height + distance.y}px`;
            this.resized.emit(this.el.nativeElement.getBoundingClientRect());
        })
    );

    sub$ = merge(this.dragMoveAudited$, of(true));

    constructor(private el: ElementRef<HTMLElement>) {}

    dragStarted(): void {
        this.startSize$.next(this.el.nativeElement.getBoundingClientRect());
    }

    dragEnded($event: CdkDragEnd): void {
        $event.source._dragRef.reset();
        this.resized.emit(this.el.nativeElement.getBoundingClientRect());
    }

    dragMoved($event: CdkDragMove): void {
        this.dragMove$.next($event);
    }
}
