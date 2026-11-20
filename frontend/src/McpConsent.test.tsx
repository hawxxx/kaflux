import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {McpConsent,type ConsentRequest} from './McpConsent';
import {setCsrf} from './api';

vi.mock('./LoginBackdrop',()=>({LoginBackdrop:()=>null,signalBackdrop:{}}));

const session={data:{user:{username:'admin',name:'Admin'},csrfToken:'csrf',demo:false,canManageMcp:true}};
const request:ConsentRequest={client:{name:'Atlas Assistant',host:'atlas.example.com'},scopes:['read','messages','write','destructive'],clusters:[{id:'prod',name:'Production'},{id:'stage',name:'Staging'}],ownerScopes:['read','messages','write'],accessTokenMinutes:60,refreshTokenDays:30};
function json(body:unknown,status=200){return new Response(JSON.stringify(body),{status,headers:{'Content-Type':'application/json'}})}
let requestResponse:()=>Response;
const assign=vi.fn();
const fetcher=vi.fn(async(url:string,init?:RequestInit)=>{
  if(url==='/api/v1/auth/session')return json(session);
  if(url==='/api/v1/mcp/oauth/requests/abc')return requestResponse();
  if(url==='/api/v1/mcp/oauth/requests/abc/approve'||url==='/api/v1/mcp/oauth/requests/abc/deny')return json({data:{redirect:'https://atlas.example.com/cb?code=c1&state=s'}});
  return json({error:{message:'unexpected '+url}},500);
});
beforeEach(()=>{requestResponse=()=>json({data:request});fetcher.mockClear();assign.mockClear();vi.stubGlobal('fetch',fetcher);vi.stubGlobal('location',{...window.location,assign});setCsrf('csrf')});
afterEach(()=>{vi.unstubAllGlobals();setCsrf('')});
const mount=()=>render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><McpConsent requestId="abc"/></QueryClientProvider>);

it('shows who is asking and what they want',async()=>{
  mount();
  expect(await screen.findByText('Atlas Assistant')).toBeVisible();
  expect(screen.getByText('atlas.example.com')).toBeVisible();
  for(const label of ['Read','Messages','Write','Destructive'])expect(screen.getByRole('checkbox',{name:new RegExp(`^${label}`)})).toBeInTheDocument();
});

it('grants the scopes the user holds and refuses the rest',async()=>{
  mount();
  const destructive=await screen.findByRole('checkbox',{name:/^Destructive/});
  expect(destructive).toBeDisabled();
  expect(screen.getByText(/does not allow this/i)).toBeVisible();
  expect(screen.getByRole('checkbox',{name:/^Write/})).toBeChecked();
});

it('approves with the chosen scopes and clusters, then follows the redirect',async()=>{
  mount();
  fireEvent.click(await screen.findByRole('checkbox',{name:/^Messages/}));
  fireEvent.click(screen.getByRole('checkbox',{name:/Production/}));
  fireEvent.click(screen.getByRole('button',{name:'Allow access'}));
  expect(await screen.findByText('successful.')).toBeVisible();
  expect(screen.getByText(/You can close this page/)).toBeVisible();
  expect(screen.getByRole('link',{name:/Continue to Atlas Assistant/})).toHaveAttribute('href','https://atlas.example.com/cb?code=c1&state=s');
  await waitFor(()=>expect(assign).toHaveBeenCalledWith('https://atlas.example.com/cb?code=c1&state=s'),{timeout:4000});
  const call=fetcher.mock.calls.find(([u])=>u.endsWith('/approve'));
  expect(JSON.parse(String(call?.[1]?.body))).toEqual({scopes:['read','write'],clusters:['prod']});
  expect((call?.[1]?.headers as Record<string,string>)['X-CSRF-Token']).toBe('csrf');
});

it('sends the session CSRF token even when nothing set it beforehand',async()=>{
  setCsrf('');
  mount();
  fireEvent.click(await screen.findByRole('button',{name:'Allow access'}));
  await waitFor(()=>expect(fetcher.mock.calls.some(([u])=>u.endsWith('/approve'))).toBe(true));
  const call=fetcher.mock.calls.find(([u])=>u.endsWith('/approve'));
  expect((call?.[1]?.headers as Record<string,string>)['X-CSRF-Token']).toBe('csrf');
});

it('denies without sending scopes',async()=>{
  mount();
  fireEvent.click(await screen.findByRole('button',{name:'Deny'}));
  expect(await screen.findByText('denied.')).toBeVisible();
  await waitFor(()=>expect(assign).toHaveBeenCalled(),{timeout:4000});
  const call=fetcher.mock.calls.find(([u])=>u.endsWith('/deny'));
  expect(JSON.parse(String(call?.[1]?.body))).toEqual({});
});

it('cannot allow with nothing selected',async()=>{
  mount();
  for(const label of [/^Read/,/^Messages/,/^Write/])fireEvent.click(await screen.findByRole('checkbox',{name:label}));
  expect(screen.getByRole('button',{name:'Allow access'})).toBeDisabled();
});

it('explains an expired request',async()=>{
  requestResponse=()=>json({error:{code:'not_found',message:'This request has expired or was already used'}},404);
  mount();
  expect(await screen.findByText(/link expired or was already used/i)).toBeVisible();
  expect(screen.queryByRole('button',{name:'Allow access'})).toBeNull();
});

it('says why a user without permission cannot approve',async()=>{
  requestResponse=()=>json({error:{code:'forbidden',message:'Connecting an MCP client requires the manage-mcp permission. Ask an administrator to grant it.'}},403);
  mount();
  expect(await screen.findByText(/requires the manage-mcp permission/i)).toBeVisible();
});
