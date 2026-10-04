import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {TopicAdministration} from './Administration';
afterEach(()=>vi.restoreAllMocks());
it('loads authoritative topic configuration before editing retention and cleanup',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>({ok:true,json:async()=>({data:{'retention.ms':'123456','cleanup.policy':'compact'}})} as Response)));
  render(<QueryClientProvider client={new QueryClient()}><TopicAdministration clusterId="demo" topic="events" session={{user:{username:'admin',role:'administrator'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
  fireEvent.click(screen.getByRole('button',{name:'Edit configuration'}));
  await waitFor(()=>expect(screen.getByLabelText('Retention · milliseconds')).toHaveValue(123456));
  expect(screen.getByLabelText('Cleanup policy')).toHaveValue('compact');
});
it('disables topic mutations for a viewer',()=>{
  render(<QueryClientProvider client={new QueryClient()}><TopicAdministration clusterId="demo" session={{user:{username:'viewer',role:'viewer'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
  expect(screen.getByRole('button',{name:'Create topic'})).toBeDisabled();
});
