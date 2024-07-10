import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RuleDetailsDialogComponent } from './violation-details-dialog.component';

describe('RuleDetailsDialogComponent', () => {
    let component: RuleDetailsDialogComponent;
    let fixture: ComponentFixture<RuleDetailsDialogComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [RuleDetailsDialogComponent]
        }).compileComponents();

        fixture = TestBed.createComponent(RuleDetailsDialogComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
