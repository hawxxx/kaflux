import {fireEvent,render,screen,waitFor,within} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {McpAccess,connectionSnippets} from './McpAccess';
import {setCsrf,type Session} from './api';

const manager:Session={user:{username:'admin',roles:['administrator']},csrfToken:'csrf',demo:false,canManageMcp:true};
const viewer:Session={user:{username:'viewer',roles:['viewer']},csrfToken:'csrf',demo:false,canManageMcp:false};
const clusters=[{id:'prod',name:'Production'},{id:'stage',name:'Staging'}];
const SECRET='kfx_mcp_'+'s'.repeat(40);
const base={ownerId:'alice',ownerProvider:'local',clusters:[] as string[],created:'2026-11-01T10:00:00Z',expires:'2026-12-01T10:00:00Z',revoked:null as string|null};
const active={...base,id:'t1',name:'laptop',scopes:['read','messages'],lastUsed:'2026-11-18T09:30:00Z',status:'active'};
const expired={...base,id:'t2',name:'old-ci',scopes:['read'],clusters:['prod'],lastUsed:null,status:'expired'};
const revoked={...base,id:'t3',name:'lost-key',scopes:['read','write'],lastUsed:null,revoked:'2026-11-10T10:00:00Z',status:'revoked'};
type Meta={enabled:boolean;writesEnabled:boolean;maxActive:number;maxExpiryDays:number};
const meta:Meta={enabled:true,writesEnabled:false,maxActive:25,maxExpiryDays:90};

let tokens:unknown[]=[];let listMeta:Meta=meta;let failList=false;
function json(body:unknown,status=200){return new Response(JSON.stringify(body),{status,headers:{'Content-Type':'application/json'}})}
const fetcher=vi.fn(async(url:string,init?:RequestInit)=>{
  const method=init?.method??'GET';
  if(url==='/api/v1/admin/mcp-tokens'&&method==='GET')return failList?json({error:{code:'store_unavailable',message:'Token inventory unavailable; retry'}},503):json({data:tokens,meta:listMeta});
  if(url==='/api/v1/admin/mcp-tokens'&&method==='POST'){const body=JSON.parse(String(init?.body));const created={...base,id:'t9',name:body.name,scopes:body.scopes,clusters:body.clusters??[],lastUsed:null,status:'active'};tokens=[created,...tokens];return json({data:{...created,secret:SECRET}},201)}
  if(url.startsWith('/api/v1/admin/mcp-tokens/')&&method==='DELETE'){const id=url.split('/').pop();tokens=tokens.map(t=>(t as {id:string}).id===id?{...(t as object),status:'revoked',revoked:'2026-11-19T10:00:00Z'}:t);return json({data:tokens.find(t=>(t as {id:string}).id===id)})}
  return json({error:{message:'unexpected '+url}},500);
});
beforeEach(()=>{tokens=[active,expired,revoked];listMeta=meta;failList=false;fetcher.mockClear();vi.stubGlobal('fetch',fetcher);setCsrf('csrf')});
afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals();setCsrf('');localStorage.clear();sessionStorage.clear()});

function mount(session:Session=manager,client=new QueryClient({defaultOptions:{queries:{retry:false}}})){
  const view=render(<QueryClientProvider client={client}><McpAccess session={session} clusters={clusters}/></QueryClientProvider>);
  return {client,...view};
}
async function fillAndCreate(name='workstation'){
  fireEvent.change(await screen.findByLabelText('Token name'),{target:{value:name}});
  fireEvent.click(screen.getByRole('button',{name:'Create token'}));
}

it('renders the disabled state when MCP is off and still lists tokens',async()=>{
  listMeta={...meta,enabled:false};mount();
  expect(await screen.findByText(/MCP is disabled by the administrator/)).toBeVisible();
  expect(await screen.findByText('laptop')).toBeVisible();
  expect(screen.queryByRole('button',{name:'Create token'})).toBeNull();
});

it('validates name, scopes and expiry before creating',async()=>{
  mount();
  const name=await screen.findByLabelText('Token name');
  fireEvent.click(screen.getByRole('button',{name:'Create token'}));
  expect(await screen.findByText('Enter a token name.')).toBeVisible();
  expect(name).toHaveAttribute('aria-invalid','true');
  fireEvent.change(name,{target:{value:'team/laptop'}});
  fireEvent.click(screen.getByRole('checkbox',{name:/^Read/}));
  const days=screen.getByRole('spinbutton',{name:'Expires in days'});
  fireEvent.change(days,{target:{value:'91'}});
  fireEvent.click(screen.getByRole('button',{name:'Create token'}));
  expect(await screen.findByText(/cannot contain "\/"/)).toBeVisible();
  expect(screen.getByText('Choose at least one scope.')).toBeVisible();
  expect(screen.getByText('Choose from 1 to 90 days.')).toBeVisible();
  expect(fetcher.mock.calls.some(([,init])=>init?.method==='POST')).toBe(false);
  expect(days).toHaveAttribute('aria-valuemax','90');
  fireEvent.keyDown(days,{key:'End'});
  expect(days).toHaveValue('90');
  fireEvent.click(screen.getByRole('button',{name:'Increase expiry'}));
  expect(days).toHaveValue('90');
  fireEvent.keyDown(days,{key:'Home'});
  expect(days).toHaveValue('1');
});

it('defaults expiry to 30 days and posts the chosen scopes and clusters',async()=>{
  mount();
  expect(await screen.findByRole('spinbutton',{name:'Expires in days'})).toHaveValue('30');
  fireEvent.click(screen.getByRole('checkbox',{name:/^Messages/}));
  fireEvent.click(screen.getByRole('checkbox',{name:/Production/}));
  await fillAndCreate('  laptop-2  ');
  await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/mcp-tokens',expect.objectContaining({method:'POST',headers:expect.objectContaining({'X-CSRF-Token':'csrf'})})));
  const post=fetcher.mock.calls.find(([,init])=>init?.method==='POST');
  expect(JSON.parse(String(post?.[1]?.body))).toEqual({name:'laptop-2',scopes:['read','messages'],clusters:['prod'],expiresInDays:30});
});

it('shows the secret once with copy and the three connection snippets',async()=>{
  const writeText=vi.fn(async()=>{});vi.stubGlobal('navigator',{...navigator,clipboard:{writeText}});
  const log=vi.spyOn(console,'log');const info=vi.spyOn(console,'info');
  mount();await fillAndCreate();
  const panel=await screen.findByRole('region',{name:'Copy your token now'});
  expect(within(panel).getByText(/shown once/i)).toBeVisible();
  expect(within(panel).getByTestId('mcp-secret')).toHaveTextContent(SECRET);
  const origin=window.location.origin;
  expect(within(panel).getByText(`claude mcp add --transport http kaflux ${origin}/mcp --header "Authorization: Bearer ${SECRET}"`)).toBeVisible();
  expect(within(panel).getByTestId('snippet-desktop').textContent).toContain(`"url": "${origin}/mcp"`);
  expect(within(panel).getByTestId('snippet-generic').textContent).toContain(`Authorization: Bearer ${SECRET}`);
  fireEvent.click(within(panel).getByRole('button',{name:'Copy secret'}));
  await waitFor(()=>expect(writeText).toHaveBeenCalledWith(SECRET));
  expect(await within(panel).findByText('Secret copied.')).toBeVisible();
  fireEvent.click(within(panel).getByRole('button',{name:'Copy Claude Code command'}));
  await waitFor(()=>expect(writeText).toHaveBeenLastCalledWith(expect.stringContaining('claude mcp add')));
  expect(await screen.findByText('workstation')).toBeVisible();
  expect(localStorage.length+sessionStorage.length).toBe(0);
  expect(window.location.href).not.toContain(SECRET);
  for(const spy of [log,info])expect(spy.mock.calls.flat().join(' ')).not.toContain(SECRET);
});

it('reports a clipboard failure and falls back when the clipboard API is missing',async()=>{
  vi.stubGlobal('navigator',{...navigator,clipboard:{writeText:vi.fn(async()=>{throw new Error('denied')})}});
  const exec=vi.fn(()=>false);(document as unknown as {execCommand:unknown}).execCommand=exec;
  mount();await fillAndCreate();
  const panel=await screen.findByRole('region',{name:'Copy your token now'});
  fireEvent.click(within(panel).getByRole('button',{name:'Copy secret'}));
  expect(await within(panel).findByText(/Copy failed/)).toBeVisible();
  expect(exec).toHaveBeenCalledWith('copy');
});

it('removes the secret after dismissal and after a remount',async()=>{
  const {client,unmount}=mount();await fillAndCreate();
  await screen.findByRole('region',{name:'Copy your token now'});
  fireEvent.click(screen.getByRole('button',{name:'I have saved the token'}));
  expect(screen.queryByRole('region',{name:'Copy your token now'})).toBeNull();
  expect(document.body.textContent).not.toContain(SECRET);
  await fillAndCreate('second');
  await screen.findByRole('region',{name:'Copy your token now'});
  unmount();
  mount(manager,client);
  expect(await screen.findByText('workstation')).toBeVisible();
  expect(document.body.textContent).not.toContain(SECRET);
  expect(JSON.stringify(client.getQueryCache().getAll().map(q=>q.state.data))).not.toContain(SECRET);
  expect(JSON.stringify(client.getMutationCache().getAll().map(m=>m.state.data))).not.toContain(SECRET);
});

it('lists tokens with status, last used and expiry',async()=>{
  mount();
  const row=(await screen.findByText('laptop')).closest('tr') as HTMLElement;
  expect(within(row).getByText('Active')).toBeVisible();
  expect(within(row).getByText('read')).toBeVisible();
  expect(within(row).getByText('messages')).toBeVisible();
  expect(within(row).getByText('All permitted clusters')).toBeVisible();
  expect(row.querySelector('time[datetime="2026-11-18T09:30:00Z"]')).not.toBeNull();
  const old=screen.getByText('old-ci').closest('tr') as HTMLElement;
  expect(within(old).getByText('Expired')).toBeVisible();
  expect(within(old).getByText('Never')).toBeVisible();
  expect(within(old).getByText('prod')).toBeVisible();
  expect(within(old).queryByRole('button',{name:/Revoke/})).toBeNull();
  const lost=screen.getByText('lost-key').closest('tr') as HTMLElement;
  expect(within(lost).getByText('Revoked')).toBeVisible();
  expect(screen.getByRole('columnheader',{name:'Owner'})).toBeInTheDocument();
});

it('asks for confirmation before revoking and refreshes the list',async()=>{
  mount();
  fireEvent.click(await screen.findByRole('button',{name:'Revoke token laptop'}));
  expect(fetcher.mock.calls.some(([,init])=>init?.method==='DELETE')).toBe(false);
  const dialog=screen.getByRole('dialog');
  expect(dialog).toHaveTextContent(/laptop/);
  fireEvent.click(within(dialog).getByRole('button',{name:'Confirm revocation'}));
  await waitFor(()=>expect(fetcher).toHaveBeenCalledWith('/api/v1/admin/mcp-tokens/t1',expect.objectContaining({method:'DELETE',headers:expect.objectContaining({'X-CSRF-Token':'csrf'})})));
  await waitFor(()=>expect(screen.queryByRole('dialog')).toBeNull());
  const row=screen.getByText('laptop').closest('tr') as HTMLElement;
  await waitFor(()=>expect(within(row).getByText('Revoked')).toBeVisible());
  expect(fetcher.mock.calls.filter(([url,init])=>url==='/api/v1/admin/mcp-tokens'&&(init?.method??'GET')==='GET').length).toBeGreaterThan(1);
  expect(screen.getByText('Token revoked.')).toBeVisible();
});

it('warns about write scopes and notes when writes are disabled',async()=>{
  mount();
  fireEvent.click(await screen.findByRole('checkbox',{name:/^Write/}));
  expect(screen.getByText(/can change your clusters/i)).toBeVisible();
  expect(screen.getAllByText(/no effect until an administrator enables MCP writes/i)[0]).toBeVisible();
});

it('omits the writes note when writes are enabled',async()=>{
  listMeta={...meta,writesEnabled:true};mount();
  fireEvent.click(await screen.findByRole('checkbox',{name:/^Destructive/}));
  expect(screen.getByText(/can delete data/i)).toBeVisible();
  expect(screen.queryByText(/no effect until an administrator enables MCP writes/i)).toBeNull();
});

it('shows the failure state with retry',async()=>{
  failList=true;mount();
  expect(await screen.findByRole('alert')).toHaveTextContent('Token inventory unavailable');
  failList=false;
  fireEvent.click(screen.getByRole('button',{name:'Retry'}));
  expect(await screen.findByText('laptop')).toBeVisible();
});

it('shows the empty state',async()=>{
  tokens=[];mount();
  expect(await screen.findByText(/No MCP tokens yet/)).toBeVisible();
});

it('hides creation and the owner column without manage-mcp',async()=>{
  mount(viewer);
  expect(await screen.findByText('laptop')).toBeVisible();
  expect(screen.queryByRole('button',{name:'Create token'})).toBeNull();
  expect(screen.getByText(/requires the manage-mcp permission/)).toBeVisible();
  expect(screen.queryByRole('columnheader',{name:'Owner'})).toBeNull();
  expect(screen.getByRole('button',{name:'Revoke token laptop'})).toBeEnabled();
});

it('falls back to session permissions when canManageMcp is absent',async()=>{
  mount({...viewer,canManageMcp:undefined,permissions:{'*':['manage-mcp']}});
  expect(await screen.findByRole('button',{name:'Create token'})).toBeVisible();
});

it('builds connection snippets from the origin',()=>{
  const s=connectionSnippets('https://kaflux.example','abc');
  expect(s.claudeCode).toBe('claude mcp add --transport http kaflux https://kaflux.example/mcp --header "Authorization: Bearer abc"');
  expect(JSON.parse(s.desktop)).toEqual({mcpServers:{kaflux:{type:'http',url:'https://kaflux.example/mcp',headers:{Authorization:'Bearer abc'}}}});
  expect(s.generic).toContain('URL: https://kaflux.example/mcp');
  expect(s.generic).toContain('Authorization: Bearer abc');
});
