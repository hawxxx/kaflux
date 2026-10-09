import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {ClusterSettings,TopicAdministration} from './Administration';
import {allPermissions} from './test-permissions';
afterEach(()=>vi.restoreAllMocks());
it('edits any broker-described topic config and sends only the changes',async()=>{
  const entries=[{name:'retention.ms',value:'123456',source:'DYNAMIC_TOPIC_CONFIG',override:true,sensitive:false},{name:'cleanup.policy',value:'compact',source:'DYNAMIC_TOPIC_CONFIG',override:true,sensitive:false},{name:'compression.type',value:'producer',source:'DEFAULT_CONFIG',override:false,sensitive:false},{name:'segment.bytes',value:'1073741824',source:'DEFAULT_CONFIG',override:false,sensitive:false}];
  const fetch=vi.fn(async(_url:string,init?:RequestInit)=>({ok:true,json:async()=>({data:init?.method==='POST'?{ok:true}:entries})} as Response));
  vi.stubGlobal('fetch',fetch);
  render(<QueryClientProvider client={new QueryClient()}><TopicAdministration clusterId="demo" topic="events" session={{user:{username:'admin',role:'administrator'},permissions:allPermissions('demo','west','east','k'),csrfToken:'test',demo:true}}/></QueryClientProvider>);
  fireEvent.click(screen.getByRole('button',{name:'Edit configuration'}));
  await waitFor(()=>expect(screen.getByLabelText(/^retention\.ms/)).toHaveValue('123456'));
  expect(screen.getByLabelText(/^cleanup\.policy/)).toHaveValue('compact');
  expect(screen.getByLabelText(/^segment\.bytes/)).toHaveValue('1073741824');
  expect(screen.getByRole('button',{name:'No changes'})).toBeDisabled();
  fireEvent.change(screen.getByLabelText(/^compression\.type/),{target:{value:'zstd'}});
  fireEvent.click(screen.getByRole('button',{name:'Reset retention.ms to default'}));
  fireEvent.click(screen.getByRole('button',{name:/Apply 2 changes/}));
  await waitFor(()=>expect(fetch).toHaveBeenCalledWith('/api/v1/clusters/demo/topics/events/config',expect.objectContaining({method:'POST'})));
  const post=fetch.mock.calls.find(([,init])=>init?.method==='POST');
  expect(JSON.parse(String(post?.[1]?.body))).toEqual({config:{'compression.type':'zstd'},reset:['retention.ms'],confirmation:true});
});
it('disables topic mutations for a viewer',()=>{
  render(<QueryClientProvider client={new QueryClient()}><TopicAdministration clusterId="demo" session={{user:{username:'viewer',role:'viewer'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
  expect(screen.getByRole('button',{name:'Create topic'})).toBeDisabled();
});
const session=(role:string)=>({user:{username:role,role},csrfToken:'test',demo:true,...(role==='administrator'?{permissions:allPermissions('demo','west','east','k')}:{})});
const clusterFixture=(id:string,name:string,configuredName=name)=>({id,name,configuredName,environment:'development',kind:'Kafka',mode:'demo',state:'healthy',brokerCount:3,topicCount:1,partitionCount:1});
it('lists every cluster and renames any of them through the backend',async()=>{
  let clusters=[clusterFixture('west','West'),clusterFixture('east','East')];
  const fetch=vi.fn(async(url:string,init?:RequestInit)=>{
    if(url.endsWith('/clusters/east/name')){const name=JSON.parse(String(init?.body)).name;clusters=clusters.map(c=>c.id==='east'?{...c,name}:c);return {ok:true,json:async()=>({data:{id:'east',name,configuredName:'East'}})} as Response}
    return {ok:true,json:async()=>({data:url.endsWith('/clusters')?clusters:{}})} as Response;
  });
  vi.stubGlobal('fetch',fetch);
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="west" session={session('administrator')}/></QueryClientProvider>);
  const rows=await screen.findAllByRole('listitem');
  expect(rows[0]).toHaveTextContent('WestCURRENT');
  fireEvent.click(screen.getByRole('button',{name:'Rename East'}));
  fireEvent.change(screen.getByLabelText('New name for East'),{target:{value:'Payments prod'}});
  fireEvent.click(screen.getByRole('button',{name:'Save'}));
  await waitFor(()=>expect(screen.getByText('Renamed to Payments prod.')).toBeInTheDocument());
  expect(screen.getByText('east · configured as East')).toBeInTheDocument();
  expect(screen.getByRole('button',{name:'Reset Payments prod to East'})).toBeEnabled();
  const call=fetch.mock.calls.find(([url])=>url.endsWith('/clusters/east/name'));
  expect(call?.[1]).toMatchObject({method:'PUT',body:JSON.stringify({name:'Payments prod'})});
});
it('shows a duplicate-name conflict and cancels with Escape',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(url:string)=>url.endsWith('/name')?{ok:false,status:409,json:async()=>({error:{code:'name_conflict',message:'Another cluster already uses this name'}})} as Response:{ok:true,json:async()=>({data:url.endsWith('/clusters')?[clusterFixture('west','West'),clusterFixture('east','East')]:{}})} as Response));
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="west" session={session('administrator')}/></QueryClientProvider>);
  fireEvent.click(await screen.findByRole('button',{name:'Rename East'}));
  fireEvent.change(screen.getByLabelText('New name for East'),{target:{value:'west'}});
  fireEvent.click(screen.getByRole('button',{name:'Save'}));
  expect(await screen.findByRole('alert')).toHaveTextContent(/already uses this name/);
  fireEvent.keyDown(screen.getByLabelText('New name for East'),{key:'Escape'});
  expect(screen.queryByLabelText('New name for East')).toBeNull();
});
it('disables cluster rename for a viewer',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(url:string)=>({ok:true,json:async()=>({data:url.endsWith('/clusters')?[clusterFixture('demo','Dev')]:{}})} as Response)));
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="demo" session={session('viewer')}/></QueryClientProvider>);
  expect(await screen.findByRole('button',{name:'Rename Dev'})).toBeDisabled();
});
it('hides the rebalancing status for clusters where it does not apply',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(url:string)=>({ok:true,json:async()=>({data:url.endsWith('/clusters')?[]:{kind:'Kafka',rebalancingStatus:'NOT_APPLICABLE'}})} as Response)));
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="k" session={{user:{username:'viewer',role:'viewer'},csrfToken:'test',demo:false}}/></QueryClientProvider>);
  await waitFor(()=>expect(screen.getByText('kind')).toBeVisible());
  expect(screen.queryByText(/rebalancing/i)).toBeNull();
});
