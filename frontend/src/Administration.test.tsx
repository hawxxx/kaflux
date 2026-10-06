import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {ClusterSettings,TopicAdministration} from './Administration';
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
