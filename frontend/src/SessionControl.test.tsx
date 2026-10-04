import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {SessionControl} from './SessionControl';
import {setCsrf} from './api';

afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals();setCsrf('')});
function mount(demo=false){
  const client=new QueryClient({defaultOptions:{queries:{retry:false}}});
  client.setQueryData(['private-records'],{value:'sensitive'});
  const signedOut=vi.fn();
  render(<QueryClientProvider client={client}><SessionControl demo={demo} onSignedOut={signedOut}/></QueryClientProvider>);
  return {client,signedOut};
}
it('posts logout with CSRF and clears private cache before returning to login',async()=>{
  setCsrf('session-csrf');
  const fetcher=vi.fn(async()=>new Response(JSON.stringify({data:{ok:true}}),{status:200}));vi.stubGlobal('fetch',fetcher);
  const {client,signedOut}=mount();fireEvent.click(screen.getByRole('button',{name:'Sign out'}));
  await waitFor(()=>expect(signedOut).toHaveBeenCalledOnce());
  expect(fetcher).toHaveBeenCalledWith('/api/v1/auth/logout',expect.objectContaining({method:'POST',credentials:'include',headers:expect.objectContaining({'X-CSRF-Token':'session-csrf'})}));
  expect(client.getQueryCache().getAll()).toHaveLength(0);
});
it('keeps authenticated data on failure and allows retry',async()=>{
  const fetcher=vi.fn().mockResolvedValueOnce(new Response(JSON.stringify({error:{message:'Database unavailable'}}),{status:503})).mockResolvedValueOnce(new Response(JSON.stringify({data:{ok:true}}),{status:200}));vi.stubGlobal('fetch',fetcher);
  const {client,signedOut}=mount();fireEvent.click(screen.getByRole('button',{name:'Sign out'}));
  expect(await screen.findByRole('alert')).toHaveTextContent('Database unavailable');
  expect(client.getQueryData(['private-records'])).toBeDefined();expect(signedOut).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole('button',{name:'Sign out'}));await waitFor(()=>expect(signedOut).toHaveBeenCalledOnce());
});
it('explains automatic demo access without a misleading logout action',()=>{
  mount(true);expect(screen.queryByRole('button',{name:'Sign out'})).not.toBeInTheDocument();expect(screen.getByText(/Demo access is automatic/)).toBeInTheDocument();
});
