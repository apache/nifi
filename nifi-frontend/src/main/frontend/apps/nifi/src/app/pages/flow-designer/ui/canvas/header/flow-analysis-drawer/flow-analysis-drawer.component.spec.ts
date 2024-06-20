import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FlowAnalysisDrawerComponent } from './flow-analysis-drawer.component';

describe('FlowAnalysisDrawerComponent', () => {
    let component: FlowAnalysisDrawerComponent;
    let fixture: ComponentFixture<FlowAnalysisDrawerComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [FlowAnalysisDrawerComponent]
        }).compileComponents();

        fixture = TestBed.createComponent(FlowAnalysisDrawerComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
