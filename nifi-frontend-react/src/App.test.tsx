import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import App from './App';

describe('App', () => {
  it('renders without crashing', () => {
    render(<App />);
    // Since we're using React Router, we expect either login or navigation elements
    const app = screen.getByRole('main', { hidden: true }) || screen.getByText(/Apache NiFi/);
    expect(app).toBeDefined();
  });
});