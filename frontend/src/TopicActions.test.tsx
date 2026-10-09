import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {clearable,DeleteTopics,TopicRowMenu} from './TopicActions';
import {validateTopicSearch} from './topic-search';
import type {Session,Topic} from './api';
import {allPermissions} from './test-permissions';
afterEach(()=>vi.restoreAllMocks());
const admin:Session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:true};
const topic:Topic={name:'orders',partitions:6,replicationFactor:3,sizeBytes:null,urp:0,cleanupPolicy:'delete',retentionMs:1000,observedAt:'',internal:false,messages:42};
const wrap=(ui:React.ReactNode)=>render(<QueryClientProvider client={new QueryClient()}>{ui}</QueryClientProvider>);
function stubFetch(data:(url:string,init?:RequestInit)=>unknown=()=>({ok:true})){
  const fetch=vi.fn(async(url:string,init?:RequestInit)=>({ok:true,json:async()=>({data:data(url,init)})} as Response));
  vi.stubGlobal('fetch',fetch);return fetch;
}
async function openMenu(){
  const trigger=screen.getByRole('button',{name:'Actions for orders'});
  fireEvent.pointerDown(trigger,{button:0,ctrlKey:false});
  await screen.findByRole('menu');
}

it('only clears topics whose cleanup policy includes delete',()=>{
  expect(clearable('delete')).toBe(true);
  expect(clearable('compact,delete')).toBe(true);
  expect(clearable('compact')).toBe(false);
  expect(clearable(undefined)).toBe(false);
});

it('shows internal topics by default and keeps the toggle in the URL state',()=>{
  expect(validateTopicSearch({}).showInternal).toBe(true);
  expect(validateTopicSearch({showInternal:'false'}).showInternal).toBe(false);
  expect(validateTopicSearch({tab:'Messages'}).tab).toBe('Messages');
  expect(validateTopicSearch({tab:'Nope'}).tab).toBeUndefined();
});

it('clears messages only after the topic name is typed',async()=>{
  const fetch=stubFetch();
  wrap(<TopicRowMenu clusterId="demo" topic={topic} session={admin} onBrowse={()=>{}} onCreated={()=>{}}/>);
  await openMenu();
  fireEvent.click(screen.getByRole('menuitem',{name:/Clear messages/}));
  const confirm=screen.getByRole('button',{name:/Confirm clearing/});
  expect(confirm).toBeDisabled();
  fireEvent.change(screen.getByLabelText('Type the topic name to confirm'),{target:{value:'orders'}});
  fireEvent.click(confirm);
  await waitFor(()=>expect(fetch).toHaveBeenCalledWith('/api/v1/clusters/demo/topics/orders/truncate',expect.objectContaining({method:'POST',body:JSON.stringify({confirmation:'orders'})})));
});

it('disables clearing compacted topics and recreating internal ones',async()=>{
  stubFetch();
  wrap(<TopicRowMenu clusterId="demo" topic={{...topic,cleanupPolicy:'compact',internal:true}} session={admin} onBrowse={()=>{}} onCreated={()=>{}}/>);
  await openMenu();
  expect(screen.getByRole('menuitem',{name:/Clear messages/})).toHaveAttribute('data-disabled');
  expect(screen.getByRole('menuitem',{name:/Recreate topic/})).toHaveAttribute('data-disabled');
  expect(screen.getByRole('menuitem',{name:/Delete topic/})).not.toHaveAttribute('data-disabled');
});

it('copies a topic with its overrides and lets the operator customise them',async()=>{
  const entries=[{name:'retention.ms',value:'1000',source:'DYNAMIC_TOPIC_CONFIG',override:true,sensitive:false},{name:'compression.type',value:'producer',source:'DEFAULT_CONFIG',override:false,sensitive:false}];
  const fetch=stubFetch((_url,init)=>init?.method==='POST'?{ok:true}:entries);
  const onCreated=vi.fn();
  wrap(<TopicRowMenu clusterId="demo" topic={topic} session={admin} onBrowse={()=>{}} onCreated={onCreated}/>);
  await openMenu();
  fireEvent.click(screen.getByRole('menuitem',{name:/Copy topic/}));
  await waitFor(()=>expect(screen.getByLabelText('retention.ms')).toHaveValue('1000'));
  expect(screen.getByLabelText('New topic name')).toHaveValue('orders-copy');
  expect(screen.getByLabelText('Partitions')).toHaveValue(6);
  fireEvent.change(screen.getByLabelText('Partitions'),{target:{value:'2'}});
  fireEvent.change(screen.getByLabelText('Setting to add'),{target:{value:'compression.type'}});
  fireEvent.click(screen.getByRole('button',{name:'Add'}));
  fireEvent.change(screen.getByLabelText('compression.type'),{target:{value:'zstd'}});
  fireEvent.click(screen.getByRole('button',{name:/Create copy/}));
  await waitFor(()=>expect(onCreated).toHaveBeenCalledWith('orders-copy'));
  const post=fetch.mock.calls.find(([,init])=>init?.method==='POST');
  expect(post?.[0]).toBe('/api/v1/clusters/demo/topics/orders/copy');
  expect(JSON.parse(String(post?.[1]?.body))).toEqual({name:'orders-copy',partitions:2,replicationFactor:3,config:{'retention.ms':'1000','compression.type':'zstd'}});
});

it('deletes the selected topics after the count phrase is typed and reports failures',async()=>{
  const fetch=vi.fn(async(url:string)=>url.endsWith('/b')?{ok:false,status:403,json:async()=>({error:{message:'Permission denied'}})} as Response:{ok:true,json:async()=>({data:{ok:true}})} as Response);
  vi.stubGlobal('fetch',fetch);
  const onDone=vi.fn();
  wrap(<DeleteTopics clusterId="demo" topics={['a','b','__consumer_offsets']} internal={['__consumer_offsets']} session={admin} onDone={onDone}/>);
  fireEvent.click(screen.getByRole('button',{name:/Delete selected/}));
  expect(screen.getByText(/includes 1 internal topic/)).toBeInTheDocument();
  const confirm=screen.getByRole('button',{name:/Confirm deletion/});
  fireEvent.change(screen.getByLabelText(/to confirm/),{target:{value:'delete 3 topic'}});
  expect(confirm).toBeDisabled();
  fireEvent.change(screen.getByLabelText(/to confirm/),{target:{value:'delete 3 topics'}});
  fireEvent.click(confirm);
  await waitFor(()=>expect(onDone).toHaveBeenCalledWith(['a','__consumer_offsets']));
  expect(fetch).toHaveBeenCalledTimes(3);
  expect(screen.getByRole('alert')).toHaveTextContent(/1 deletion failed: b/);
});

it('disables bulk deletion for a viewer',()=>{
  wrap(<DeleteTopics clusterId="demo" topics={['a']} internal={[]} session={{user:{username:'v',role:'viewer'},csrfToken:'t',demo:true}} onDone={()=>{}}/>);
  expect(screen.getByRole('button',{name:/Delete selected/})).toBeDisabled();
});
