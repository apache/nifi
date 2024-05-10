import { Component, inject } from '@angular/core';
import { CommonModule } from '@angular/common';
import { filter } from 'rxjs';
import { MatDialogRef } from '@angular/material/dialog';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';

@Component({
    selector: 'close-on-escape-dialog',
    standalone: true,
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
                    if (!this.isDirty()) {
                        this.dialogRef.close();
                    }
                });
        }
    }

    isDirty(): boolean {
        return false;
    }
}
