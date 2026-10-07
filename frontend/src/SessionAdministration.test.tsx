import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {SessionAdministration} from './SessionAdministration';
import {api,setCsrf,type Session} from './api';
const admin:Session={user:{username:'admin',roles:['administrator']},csrfToken:'csrf',demo:false};
const row={handle:'a'.repeat(64),userId:'alice',provider:'local',roles:['viewer'],expires:'2026-10-04T12:00:00Z',current:false};
afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals();setCsrf('')});
function mount(session=admin){const client=new QueryClient({defaultOptions:{queries:{retry:false}}});client.setQueryData(['private'], 'secret');const signedOut=vi.fn();render(<QueryClientProvider client={client}><SessionAdministration session={session} onSignedOut={signedOut}/></QueryClientProvider>);return {client,signedOut}}
it('lists bounded pages and requires explicit confirmation before revoking',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/revoke')?{data:{ok:true,current:false}}:{data:[row],meta:{limit:25,offset:url.includes('offset=25')?25:0,hasMore:!url.includes('offset=25')}})));vi.stubGlobal('fetch',fetcher);setCsrf('csrf');mount();
 expect(await screen.findByText('alice')).toBeVisible();fireEvent.click(screen.getByRole('button',{name:'Next page'}));await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/sessions?limit=25&offset=25',expect.anything()));
 fireEvent.click(await screen.findByRole('button',{name:'Revoke session for alice'}));expect(fetcher.mock.calls.some(([url])=>url.includes('/revoke'))).toBe(false);
 fireEvent.click(screen.getByRole('button',{name:'Confirm revocation'}));await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/sessions/'+row.handle+'/revoke',expect.objectContaining({method:'POST',body:'{"confirm":true}',headers:expect.objectContaining({'X-CSRF-Token':'csrf'})})));
});
it('clears private cache and CSRF after current session revocation',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/revoke')?{data:{ok:true,current:true}}:{data:[{...row,current:true}],meta:{limit:25,offset:0,hasMore:false}})));vi.stubGlobal('fetch',fetcher);setCsrf('csrf');const {client,signedOut}=mount();await screen.findByText('alice');fireEvent.click(screen.getByRole('button',{name:'Revoke session for alice'}));expect(screen.getByText(/You will be signed out/)).toBeVisible();fireEvent.click(screen.getByRole('button',{name:'Confirm revocation'}));await waitFor(()=>expect(signedOut).toHaveBeenCalledOnce());expect(client.getQueryData(['private'])).toBeUndefined();await api('/probe',{method:'POST'});expect(fetcher).toHaveBeenLastCalledWith('/api/v1/probe',expect.objectContaining({headers:expect.objectContaining({'X-CSRF-Token':''})}));
});
it('shows retryable load and revoke failures without discarding private data',async()=>{
 let fail=true;const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(fail||url.includes('/revoke')?{error:{message:'Session store unavailable'}}:{data:[row],meta:{limit:25,offset:0,hasMore:false}}),{status:fail||url.includes('/revoke')?503:200}));vi.stubGlobal('fetch',fetcher);const {client,signedOut}=mount();expect(await screen.findByRole('alert')).toHaveTextContent('Session store unavailable');fail=false;fireEvent.click(screen.getByRole('button',{name:'Retry'}));await screen.findByText('alice');fireEvent.click(screen.getByRole('button',{name:'Revoke session for alice'}));fireEvent.click(screen.getByRole('button',{name:'Confirm revocation'}));expect(await screen.findByRole('alert')).toHaveTextContent('Session store unavailable');expect(client.getQueryData(['private'])).toBe('secret');expect(signedOut).not.toHaveBeenCalled();
});
it('blocks non-admin access without fetching and explains demo access',()=>{const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);mount({...admin,user:{username:'viewer',roles:['viewer']}});expect(screen.getByRole('alert')).toHaveTextContent('Administrator access required');expect(fetcher).not.toHaveBeenCalled()});
it('explains demo automatic access and offers no revoke action',()=>{vi.stubGlobal('fetch',vi.fn());mount({...admin,demo:true});expect(screen.getByText(/Demo access is automatic/)).toBeVisible();expect(screen.queryByRole('button',{name:/Revoke/})).toBeNull()});
it('shows an empty session list',async()=>{vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[],meta:{limit:25,offset:0,hasMore:false}}))));mount();expect(await screen.findByText('No active sessions.')).toBeVisible()});

it('allows configured session administration capability without administrator role',async()=>{vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[],meta:{limit:25,offset:0,hasMore:false}}))));mount({...admin,user:{username:'security',roles:['session-manager']},canManageSessions:true});expect(await screen.findByText('No active sessions.')).toBeVisible()});
it('honors explicit capability denial even for an administrator role',()=>{const fetcher=vi.fn();vi.stubGlobal('fetch',fetcher);mount({...admin,canManageSessions:false});expect(screen.getByRole('alert')).toHaveTextContent('Administrator access required');expect(fetcher).not.toHaveBeenCalled()});

const other={...row,handle:'b'.repeat(64),expires:'2026-10-05T09:00:00Z'};
it('groups repeated sessions for the same user into one row with a count and the newest expiry',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[row,other],meta:{limit:25,offset:0,hasMore:false}}))));mount();
 await screen.findByText('alice');
 expect(screen.getAllByText('alice')).toHaveLength(1);
 expect(screen.getByText('2')).toBeVisible();
 expect(screen.getByText('10/5/2026, 3:00:00 AM')).toBeVisible();
 expect(screen.getByText('1 users',{exact:false})).toBeVisible();
});
it('keeps a single active session for a user collapsed into its own session row without an expand control',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[row],meta:{limit:25,offset:0,hasMore:false}}))));mount();
 await screen.findByText('alice');
 expect(screen.queryByRole('button',{name:/Expand sessions/})).toBeNull();
 expect(screen.getByRole('button',{name:'Revoke session for alice'})).toBeVisible();
});
it('expands a grouped user to reveal and individually revoke each underlying session',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/revoke')?{data:{ok:true,current:false}}:{data:[row,other],meta:{limit:25,offset:0,hasMore:false}})));vi.stubGlobal('fetch',fetcher);setCsrf('csrf');mount();
 await screen.findByText('alice');
 expect(screen.queryByRole('button',{name:/Revoke session for alice/})).toBeNull();
 fireEvent.click(screen.getByRole('button',{name:'Expand sessions for alice'}));
 const revokeButtons=screen.getAllByRole('button',{name:'Revoke session for alice'});
 expect(revokeButtons).toHaveLength(2);
 fireEvent.click(revokeButtons[0]);
 fireEvent.click(screen.getByRole('button',{name:'Confirm revocation'}));
 await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/sessions/'+row.handle+'/revoke',expect.objectContaining({method:'POST'})));
});
it('marks the active session distinctly from a user\'s other sessions once expanded',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[{...row,current:true},other],meta:{limit:25,offset:0,hasMore:false}}))));mount();
 await screen.findByText('alice');
 expect(screen.getByText('Current session')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Expand sessions for alice'}));
 expect(screen.getByText('This session')).toBeVisible();
});
it('never lists an expired session even when a live session for the same user is present',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[row],meta:{limit:25,offset:0,hasMore:false}}))));mount();
 await screen.findByText('alice');
 expect(screen.getByText('1')).toBeVisible();
});
it('revokes every other session for a user in one bulk action while leaving the current session untouched',async()=>{
 const fetcher=vi.fn(async(url:string)=>new Response(JSON.stringify(url.includes('/revoke')?{data:{ok:true,current:false}}:{data:[{...row,current:true},other],meta:{limit:25,offset:0,hasMore:false}})));vi.stubGlobal('fetch',fetcher);setCsrf('csrf');mount();
 await screen.findByText('alice');
 fireEvent.click(screen.getByRole('button',{name:'Revoke other sessions for alice'}));
 expect(screen.getByText(/current session is not affected/)).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Confirm revocation'}));
 await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/sessions/'+other.handle+'/revoke',expect.objectContaining({method:'POST'})));
 expect(fetcher.mock.calls.some(([url])=>url==='/api/v1/admin/sessions/'+row.handle+'/revoke')).toBe(false);
});
it('offers no bulk revoke action for a user with only one active session',async()=>{
 vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({data:[row],meta:{limit:25,offset:0,hasMore:false}}))));mount();
 await screen.findByText('alice');
 expect(screen.queryByRole('button',{name:/Revoke other sessions/})).toBeNull();
});
