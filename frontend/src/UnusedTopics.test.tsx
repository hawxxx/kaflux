import {fireEvent,render,screen,waitFor,within} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {UnusedTopics,unusedCsv} from './UnusedTopics';
import {allPermissions} from './test-permissions';
const session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:true};
const wrap=(ui:React.ReactNode)=>render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}>{ui}</QueryClientProvider>);
afterEach(()=>vi.restoreAllMocks());
const row=(name:string,classification:string,extra:object={})=>({name,classification,reasons:[`why ${name}`],partitions:3,emptyPartitions:[],records:10,sizeBytes:2048,lastRecordAt:'2026-09-01T00:00:00Z',consumerGroups:[],activeConsumers:0,internal:false,ageKnown:true,fingerprint:`fp-${name}`,...extra});
const topics=[row('a.empty','unused',{records:0,lastRecordAt:null,ageKnown:false}),row('b.old','likely-unused',{emptyPartitions:[1,2]}),row('c.idle','idle',{consumerGroups:['g1']}),row('_int','unused',{internal:true})];
type Call={path:string;method?:string;body:any};
function stub(calls:Call[],handler:(path:string,method?:string)=>{ok?:boolean;data?:unknown;error?:{code:string;message:string}}){vi.stubGlobal('fetch',vi.fn(async(path:string,o?:RequestInit)=>{calls.push({path,method:o?.method,body:o?.body?JSON.parse(String(o.body)):null});const r=handler(path,o?.method);return {ok:r.ok!==false,status:r.ok===false?400:200,json:async()=>r.ok===false?{error:r.error}:{data:r.data}} as Response}))}
const report=(t=topics)=>({topics:t,evaluatedAt:'2026-11-13T00:00:00Z',partial:false});
describe('unused topics',()=>{
  it('filters rows with classification chips and shows counts',async()=>{
    stub([],()=>({data:report()}));
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    expect(await screen.findByText('a.empty')).toBeInTheDocument();
    expect(screen.getByRole('button',{name:'Likely unused (1)'})).toBeInTheDocument();
    expect(screen.getByTitle(/Kafka cannot delete partitions/)).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button',{name:'Idle (1)'}));
    expect(screen.queryByText('a.empty')).toBeNull();expect(screen.getByText('c.idle')).toBeInTheDocument();
  });
  it('selection count drives the phrase and the request carries topics, age and confirmation',async()=>{
    const calls:Call[]=[];stub(calls,(p,m)=>m==='POST'?{data:{deleted:['a.empty'],skipped:[{topic:'b.old',reason:'topic has an active consumer'}]}}:{data:report()});
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    await screen.findByText('a.empty');
    fireEvent.click(screen.getByRole('button',{name:'Select all unused'}));
    // The internal topic is never selected.
    expect(screen.getByRole('button',{name:'Delete selected (1)'})).toBeEnabled();
    fireEvent.click(screen.getByLabelText('Select b.old'));
    fireEvent.click(screen.getByRole('button',{name:'Delete selected (2)'}));
    expect(await screen.findByText('delete 2 topics')).toBeInTheDocument();
    const confirm=screen.getByRole('button',{name:'Confirm deletion'});expect(confirm).toBeDisabled();
    fireEvent.change(screen.getByLabelText(/to confirm/),{target:{value:'delete 2 topics'}});
    fireEvent.click(confirm);
    await waitFor(()=>expect(calls.some(c=>c.method==='POST'&&c.path.endsWith('/topics/bulk-delete')&&c.body.minAgeDays===30&&c.body.confirmation==='delete 2 topics'&&c.body.fingerprints['a.empty']==='fp-a.empty'&&c.body.fingerprints['b.old']==='fp-b.old'&&c.body.includeEmptyUnknownAge===false&&c.body.topics.join()==='a.empty,b.old')).toBe(true));
    const dialog=await screen.findByRole('dialog');
    expect(await within(dialog).findByText(/Skipped: topic has an active consumer/)).toBeInTheDocument();
    expect(within(dialog).getByText(/1 deleted, 1 skipped/)).toBeInTheDocument();
  });
  it('gates includeEmptyUnknownAge behind an unchecked checkbox and flags unknown age',async()=>{
    const calls:Call[]=[];stub(calls,(p,m)=>m==='POST'?{data:{deleted:['a.empty'],skipped:[]}}:{data:report()});
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    await screen.findByText('a.empty');
    expect(screen.getByText(/age unknown — may be brand new/)).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('Select a.empty'));
    fireEvent.click(screen.getByRole('button',{name:'Delete selected (1)'}));
    const box=await screen.findByLabelText(/Also delete empty topics whose age is unknown/);
    expect(box).not.toBeChecked();
    fireEvent.click(box);
    fireEvent.change(screen.getByLabelText(/to confirm/),{target:{value:'delete 1 topics'}});
    fireEvent.click(screen.getByRole('button',{name:'Confirm deletion'}));
    await waitFor(()=>expect(calls.some(c=>c.method==='POST'&&c.body.includeEmptyUnknownAge===true&&c.body.fingerprints['a.empty']==='fp-a.empty')).toBe(true));
  });
  it('shows the partial reason and a 503 from the report',async()=>{
    stub([],()=>({data:{...report(),partial:true,partialReason:'evaluation deadline reached: 4 topics not evaluated'}}));
    const first=wrap(<UnusedTopics clusterId="demo" session={session}/>);
    expect(await screen.findByText(/evaluation deadline reached: 4 topics not evaluated/)).toBeInTheDocument();
    first.unmount();
    stub([],()=>({ok:false,error:{code:'usage_unavailable',message:'Consumer group state could not be read; no topic can be judged unused'}}));
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    expect(await screen.findByText(/Consumer group state could not be read/)).toBeInTheDocument();
  });
  it('disables deletion without the delete permission and never selects internal topics',async()=>{
    stub([],()=>({data:report()}));
    wrap(<UnusedTopics clusterId="demo" session={{...session,permissions:{demo:['read']}}}/>);
    await screen.findByText('a.empty');
    fireEvent.click(screen.getByLabelText('Select a.empty'));
    expect(screen.getByRole('button',{name:'Delete selected (1)'})).toBeDisabled();
    expect(screen.getByLabelText('Select _int')).toBeDisabled();
  });
  it('sends the threshold, internal flag and exclude pattern, and blocks invalid regexes locally',async()=>{
    const calls:Call[]=[];stub(calls,()=>({data:report()}));
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    await screen.findByText('a.empty');
    fireEvent.change(screen.getByLabelText('Unused for at least (days)'),{target:{value:'90'}});
    await waitFor(()=>expect(calls.some(c=>c.path.includes('minAgeDays=90'))).toBe(true));
    fireEvent.change(screen.getByLabelText(/Exclude names/),{target:{value:'('}});
    expect(await screen.findByText(/Not a valid regular expression/)).toBeInTheDocument();
    await new Promise(r=>setTimeout(r,500));
    expect(calls.some(c=>c.path.includes('exclude=%28'))).toBe(false);
    fireEvent.change(screen.getByLabelText(/Exclude names/),{target:{value:'^tmp'}});
    await waitFor(()=>expect(calls.some(c=>c.path.includes('exclude=%5Etmp'))).toBe(true));
    expect(screen.queryByText(/Not a valid regular expression/)).toBeNull();
  });
  it('filters by name, caps the selection at 100 and keeps hidden selections when deselecting shown rows',async()=>{
    const many=Array.from({length:120},(_,i)=>row(`t${String(i).padStart(3,'0')}`,'likely-unused'));
    stub([],()=>({data:report(many)}));
    wrap(<UnusedTopics clusterId="demo" session={session}/>);
    await screen.findByText('t000');
    fireEvent.click(screen.getByRole('button',{name:'Select all likely unused'}));
    expect(screen.getByRole('button',{name:'Delete selected (100)'})).toBeEnabled();
    expect(screen.getByText(/Selection limit reached/)).toBeInTheDocument();
    expect(screen.getByLabelText('Select t110')).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Filter topics by name'),{target:{value:'t00'}});
    fireEvent.click(screen.getByLabelText('Select all shown'));
    expect(screen.getByRole('button',{name:'Delete selected (90)'})).toBeEnabled();
  });
  it('exports the current result as CSV',()=>{
    const csv=unusedCsv([row('x,y','idle',{reasons:['a; b']}) as any]);
    expect(csv.split('\n')[1]).toContain('"x,y",idle');
    expect(csv.split('\n')[0]).toContain('lastRecordAt');
  });
});
