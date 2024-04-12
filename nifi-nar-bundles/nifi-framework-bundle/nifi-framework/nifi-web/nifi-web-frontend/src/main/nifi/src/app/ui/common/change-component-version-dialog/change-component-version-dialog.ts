import { Component, EventEmitter, Inject, Output } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule } from '@angular/material/dialog';
import { MatButton } from '@angular/material/button';
import { OpenChangeComponentVersionDialogRequest } from '../../../pages/flow-designer/state/flow';
import { Bundle, DocumentedType } from '../../../state/shared';
import { MatFormField, MatLabel, MatOption, MatSelect } from '@angular/material/select';
import { FormBuilder, FormControl, FormGroup, ReactiveFormsModule, Validators } from '@angular/forms';
import { TextTip } from '../tooltips/text-tip/text-tip.component';
import { NifiTooltipDirective } from '../tooltips/nifi-tooltip.directive';

@Component({
    selector: 'change-component-version-dialog',
    standalone: true,
    imports: [
        MatDialogModule,
        MatButton,
        MatSelect,
        MatLabel,
        MatOption,
        MatFormField,
        ReactiveFormsModule,
        NifiTooltipDirective
    ],
    templateUrl: './change-component-version-dialog.html',
    styleUrl: './change-component-version-dialog.scss'
})
export class ChangeComponentVersionDialog {
    versions: DocumentedType[];
    selected: DocumentedType | null = null;
    changeComponentVersionForm: FormGroup;
    private currentBundle: Bundle;

    @Output() changeVersion: EventEmitter<DocumentedType> = new EventEmitter<DocumentedType>();

    constructor(
        @Inject(MAT_DIALOG_DATA) private dialogRequest: OpenChangeComponentVersionDialogRequest,
        private formBuilder: FormBuilder
    ) {
        this.versions = dialogRequest.componentVersions;
        this.currentBundle = dialogRequest.fetchRequest.bundle;
        const idx = this.versions.findIndex(
            (version: DocumentedType) => version.bundle.version === this.currentBundle.version
        );
        this.selected = this.versions[idx > 0 ? idx : 0];
        this.changeComponentVersionForm = this.formBuilder.group({
            bundle: new FormControl(this.selected, [Validators.required])
        });
    }

    apply(): void {
        if (this.selected) {
            this.changeVersion.next(this.selected);
        }
    }

    isCurrent(selection: DocumentedType | null): boolean {
        return selection?.bundle.version === this.currentBundle.version;
    }

    protected readonly TextTip = TextTip;
}
