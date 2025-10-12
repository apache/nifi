import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import { Provider } from 'react-redux';
import { BrowserRouter } from 'react-router-dom';
import { configureStore } from '@reduxjs/toolkit';
import Navigation from './Navigation';
import currentUserSlice from '../state/currentUserSlice';

const createTestStore = () => configureStore({
  reducer: {
    currentUser: currentUserSlice
  },
  preloadedState: {
    currentUser: {
      user: {
        identity: 'testuser@example.com'
      },
      status: 'success' as const,
      error: null
    }
  }
});

describe('Navigation', () => {
  it('renders navigation items', () => {
    const store = createTestStore();
    
    render(
      <Provider store={store}>
        <BrowserRouter>
          <Navigation />
        </BrowserRouter>
      </Provider>
    );

    expect(screen.getByText('Apache NiFi')).toBeDefined();
    expect(screen.getByText('Flow Designer')).toBeDefined();
    expect(screen.getByText('Data Provenance')).toBeDefined();
  });

  it('shows user information when logged in', () => {
    const store = createTestStore();
    
    render(
      <Provider store={store}>
        <BrowserRouter>
          <Navigation />
        </BrowserRouter>
      </Provider>
    );

    expect(screen.getByText('testuser@example.com')).toBeDefined();
    expect(screen.getByText('Authenticated')).toBeDefined();
  });
});