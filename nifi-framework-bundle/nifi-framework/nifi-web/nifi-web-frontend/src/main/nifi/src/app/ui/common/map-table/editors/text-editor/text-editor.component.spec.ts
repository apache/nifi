import { ComponentFixture, TestBed } from '@angular/core/testing';
import { TextEditor } from './text-editor.component';

describe('TextEditor', () => {
    let component: TextEditor;
    let fixture: ComponentFixture<TextEditor>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [TextEditor]
        }).compileComponents();

        fixture = TestBed.createComponent(TextEditor);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
