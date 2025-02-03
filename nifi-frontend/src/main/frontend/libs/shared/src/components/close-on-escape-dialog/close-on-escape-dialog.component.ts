/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { filter } from 'rxjs';
import { MatDialogRef } from '@angular/material/dialog';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'close-on-escape-dialog',
    imports: [CommonModule],
    template: ''
})
export abstract class CloseOnEscapeDialog {
    private dialogRef: MatDialogRef<CloseOnEscapeDialog> = inject(MatDialogRef);

    protected constructor() {
        if (this.dialogRef) {
            this.dialogRef
                .keydownEvents()
                .pipe(
                    filter((event: KeyboardEvent) => event.key === 'Escape'),
                    takeUntilDestroyed()
                )
                .subscribe(() => {
                    if (this.getCancelDialogResult()) {
                        this.dialogRef.close(this.getCancelDialogResult());
                    } else {
                        this.dialogRef.close();
                    }
                });
        }
    }

    getCancelDialogResult(): any | null | undefined {
        return null;
    }

    isDirty(): boolean {
        return false;
    }
}
