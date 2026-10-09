import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {ClusterSettings,TopicAdministration} from './Administration';
afterEach(()=>vi.restoreAllMocks());
it('edits any broker-described topic config and sends only the changes',async()=>{
  const entries=[{name:'retention.ms',value:'123456',source:'DYNAMIC_TOPIC_CONFIG',override:true,sensitive:false},{name:'cleanup.policy',value:'compact',source:'DYNAMIC_TOPIC_CONFIG',override:true,sensitive:false},{name:'compression.type',value:'producer',source:'DEFAULT_CONFIG',override:false,sensitive:false},{name:'segment.bytes',value:'1073741824',source:'DEFAULT_CONFIG',override:false,sensitive:false}];
  const fetch=vi.fn(async(_url:string,init?:RequestInit)=>({ok:true,json:async()=>({data:init?.method==='POST'?{ok:true}:entries})} as Response));
  vi.stubGlobal('fetch',fetch);
  render(<QueryClientProvider client={new QueryClient()}><TopicAdministration clusterId="demo" topic="events" session={{user:{username:'admin',role:'administrator'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
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
it('renames a cluster through the backend',async()=>{
  const fetch=vi.fn(async(url:string,init?:RequestInit)=>({ok:true,json:async()=>({data:url.endsWith('/name')?JSON.parse(String(init?.body)):{}})} as Response));
  vi.stubGlobal('fetch',fetch);
  const cluster={id:'demo',name:'Development simulator',configuredName:'Development simulator',environment:'development',kind:'Kafka',mode:'demo',state:'healthy',brokerCount:3,topicCount:1,partitionCount:1};
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="demo" cluster={cluster} session={{user:{username:'admin',role:'administrator'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
  fireEvent.change(screen.getByLabelText('Cluster name'),{target:{value:'Payments prod'}});
  fireEvent.click(screen.getByRole('button',{name:'Save name'}));
  await waitFor(()=>expect(screen.getByText('Cluster name saved.')).toBeInTheDocument());
  const call=fetch.mock.calls.find(([url])=>url.endsWith('/clusters/demo/name'));
  expect(call?.[1]).toMatchObject({method:'PUT',body:JSON.stringify({name:'Payments prod'})});
});
it('disables cluster rename for a viewer',()=>{
  const cluster={id:'demo',name:'Dev',configuredName:'Dev',environment:'development',kind:'Kafka',mode:'demo',state:'healthy',brokerCount:3,topicCount:1,partitionCount:1};
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="demo" cluster={cluster} session={{user:{username:'viewer',role:'viewer'},csrfToken:'test',demo:true}}/></QueryClientProvider>);
  expect(screen.getByLabelText('Cluster name')).toBeDisabled();
});
it('hides the rebalancing status for clusters where it does not apply',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>({ok:true,json:async()=>({data:{kind:'Kafka',rebalancingStatus:'NOT_APPLICABLE'}})} as Response)));
  const cluster={id:'k',name:'K',configuredName:'K',environment:'development',kind:'Kafka',mode:'live',state:'healthy',brokerCount:3,topicCount:1,partitionCount:1};
  render(<QueryClientProvider client={new QueryClient()}><ClusterSettings clusterId="k" cluster={cluster} session={{user:{username:'viewer',role:'viewer'},csrfToken:'test',demo:false}}/></QueryClientProvider>);
  await waitFor(()=>expect(screen.getByText('kind')).toBeVisible());
  expect(screen.queryByText(/rebalancing/i)).toBeNull();
});
