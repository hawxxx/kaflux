import {fireEvent,render,screen,within} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {Messages} from './Messages';

const detail={name:'orders',partitions:[{id:0,leader:1,replicas:[1],isr:[1],startOffset:10,endOffset:200,sizeBytes:null},{id:1,leader:1,replicas:[1],isr:[1],startOffset:0,endOffset:5,sizeBytes:null}],replicationFactor:1,sizeBytes:null,urp:0,cleanupPolicy:'delete',retentionMs:null,observedAt:''};
const records=[
  {partition:0,offset:150,timestamp:'2026-10-03T00:00:00Z',key:'order-1',value:'{"total": 12}',valueBase64:btoa('{"total": 12}'),headers:[{key:'trace-id',value:'abc'}]},
  {partition:0,offset:151,timestamp:'2026-10-03T00:00:01Z',key:'order-2',value:'plain',valueBase64:btoa('plain'),headers:[]},
];
function mount(fetch:ReturnType<typeof vi.fn>){
  vi.stubGlobal('fetch',fetch);
  render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><Messages clusterId="demo" initialTopic="orders" canProduce/></QueryClientProvider>);
}
const respond=(url:string)=>({ok:true,json:async()=>({data:url.includes('/messages?')?records:url.includes('/topics/')?detail:[]})} as Response);
afterEach(()=>vi.unstubAllGlobals());
describe('topic messages',()=>{
  it('reads the newest records of the partition and keeps payloads collapsed',async()=>{
    const fetch=vi.fn(async(url:string)=>respond(url));mount(fetch);
    expect(await screen.findByText('order-1')).toBeVisible();
    const read=new URL(String(fetch.mock.calls.map(c=>c[0]).find(u=>String(u).includes('/messages?'))),'http://x').searchParams;
    expect(read.get('offsets')).toBe('0:150,1:0');
    expect(read.has('partition')).toBe(false);
    expect(screen.getByText('{"total":12}')).toBeVisible();
    expect(document.querySelector('.record-detail')).toBeNull();
    expect(screen.queryByRole('combobox',{name:'Topic'})).toBeNull();
  });
  it('expands a record into value, key and header views',async()=>{
    mount(vi.fn(async(url:string)=>respond(url)));
    fireEvent.click((await screen.findByText('order-1')).closest('button')!);
    const detailView=document.querySelector('.record-detail') as HTMLElement;
    expect(within(detailView).getByText('12')).toBeVisible();
    fireEvent.click(within(detailView).getByRole('tab',{name:'Headers · 1'}));
    expect(within(detailView).getByRole('cell',{name:'trace-id'})).toBeVisible();
    fireEvent.click(screen.getByRole('button',{name:'Expand all'}));
    expect(document.querySelectorAll('.record-detail')).toHaveLength(2);
  });
  it('filters returned records by the chosen part',async()=>{
    mount(vi.fn(async(url:string)=>respond(url)));
    await screen.findByText('order-1');
    fireEvent.change(screen.getByLabelText('Search returned records'),{target:{value:'trace'}});
    expect(document.querySelectorAll('.record')).toHaveLength(1);
    fireEvent.change(screen.getByLabelText('Search in'),{target:{value:'key'}});
    expect(screen.getByText('No returned records match this search.')).toBeVisible();
  });
  it('opens the producer prefilled from a returned record',async()=>{
    mount(vi.fn(async(url:string)=>respond(url)));
    fireEvent.click((await screen.findByText('order-1')).closest('button')!);
    fireEvent.click(screen.getByRole('button',{name:'Produce copy'}));
    expect(await screen.findByLabelText('Message key')).toHaveValue('order-1');
    expect(screen.getByLabelText('Message value')).toHaveValue('{"total": 12}');
    expect(screen.getByLabelText('Header 1 name')).toHaveValue('trace-id');
  });
  it('reads every partition in one request by default and labels each record with its partition',async()=>{
    const fetch=vi.fn(async(url:string)=>respond(url));mount(fetch);
    await screen.findByText('order-1');
    const reads=fetch.mock.calls.map(c=>new URL(String(c[0]),'http://x').searchParams).filter(q=>q.has('limit'));
    expect(reads).toHaveLength(1);
    expect(reads[0].get('offsets')).toBe('0:150,1:0');
    expect(screen.getByLabelText('Partition')).toHaveValue('all');
    expect(document.querySelectorAll('.record')).toHaveLength(2);
    expect(screen.getAllByTitle(/^Partition 0, offset/)).toHaveLength(2);
    expect(screen.queryByRole('button',{name:'Offset'})).toBeNull();
  });
  it('tails each partition from where it was read, not past records the server did not return',async()=>{
    Element.prototype.scrollTo=vi.fn();
    const fetch=vi.fn(async(url:string)=>respond(url));mount(fetch);
    await screen.findByText('order-1');fetch.mockClear();
    fireEvent.click(screen.getByRole('button',{name:'Start live tail'}));
    await vi.waitFor(()=>expect(fetch.mock.calls.some(c=>String(c[0]).includes('/messages?'))).toBe(true));
    const read=new URL(String(fetch.mock.calls.map(c=>c[0]).find(u=>String(u).includes('/messages?'))),'http://x').searchParams;
    expect(read.get('offsets')).toBe('0:152,1:0');
  });
  it('reads a single partition once chosen',async()=>{
    const fetch=vi.fn(async(url:string)=>respond(url));mount(fetch);
    await screen.findByText('order-1');fetch.mockClear();
    fireEvent.change(screen.getByLabelText('Partition'),{target:{value:'1'}});
    await vi.waitFor(()=>expect(fetch.mock.calls.some(c=>String(c[0]).includes('/messages?'))).toBe(true));
    const reads=fetch.mock.calls.map(c=>new URL(String(c[0]),'http://x').searchParams).filter(q=>q.has('limit'));
    expect(reads).toHaveLength(1);
    expect(reads[0].get('partition')).toBe('1');
    expect(reads[0].has('offsets')).toBe(false);
  });
  it('says what it is reading while the first records load',async()=>{
    mount(vi.fn((url:string)=>String(url).includes('/messages?')?new Promise<Response>(()=>{}):Promise.resolve(respond(url))));
    expect(await screen.findByText('Reading every partition…')).toBeVisible();
    expect(screen.getByRole('progressbar',{name:'Reading every partition'})).toBeVisible();
  });
  it('scans the topic on the server, continues where a budget stopped and returns to records',async()=>{
    const found={partition:1,offset:3,timestamp:'2026-10-02T00:00:00Z',key:'dev',value:'mac aa:bb',valueBase64:btoa('mac aa:bb'),headers:[]};
    const later={...found,partition:0,offset:190,key:'dev-2'};
    const searches:any[]=[];
    const fetch=vi.fn(async(url:string,init?:RequestInit)=>{
      if(!String(url).includes('/messages/search'))return respond(url);
      const body=JSON.parse(String(init?.body));searches.push(body);
      const data=searches.length===1?{matches:[found],scanned:{records:2000000,bytes:1024},resume:{'0':160},done:false,stoppedBy:'records'}:{matches:[later],scanned:{records:40,bytes:10},resume:{},done:true,stoppedBy:'end'};
      return {ok:true,json:async()=>({data})} as Response;
    });
    mount(fetch);
    await screen.findByText('order-1');
    expect(screen.getByRole('button',{name:'Scan topic'})).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Search returned records'),{target:{value:' aa:bb '}});
    fireEvent.change(screen.getByLabelText('Search in'),{target:{value:'value'}});
    fireEvent.click(screen.getByRole('button',{name:'Scan topic'}));
    expect(await screen.findByText('1 match for “aa:bb”')).toBeVisible();
    expect(searches[0]).toEqual({topic:'orders',match:{in:'value',text:'aa:bb'}});
    expect(screen.getByText('Stopped after the record limit')).toBeVisible();
    expect(screen.getByText(/Scanned 2,000,000 records/)).toBeVisible();
    expect(screen.getByRole('button',{name:'Start live tail'})).toBeDisabled();
    fireEvent.click(screen.getByRole('button',{name:'Continue scanning'}));
    expect(await screen.findByText('2 matches for “aa:bb”')).toBeVisible();
    expect(searches[1]).toEqual({topic:'orders',from:{offsets:{'0':160}},match:{in:'value',text:'aa:bb'}});
    expect(screen.getByText('Scanned to the end of the range')).toBeVisible();
    expect(screen.queryByRole('button',{name:'Continue scanning'})).toBeNull();
    expect(screen.getByText('dev-2')).toBeVisible();
    fireEvent.click(screen.getByRole('button',{name:'Back to records'}));
    expect(screen.queryByText(/matches for/)).toBeNull();
  });
  it('cancels a running scan',async()=>{
    let signal:AbortSignal|undefined;
    mount(vi.fn((url:string,init?:RequestInit)=>{
      if(!String(url).includes('/messages/search'))return Promise.resolve(respond(url));
      signal=init?.signal??undefined;
      return new Promise<Response>((_,reject)=>signal!.addEventListener('abort',()=>reject(new DOMException('aborted','AbortError'))));
    }));
    await screen.findByText('order-1');
    fireEvent.change(screen.getByLabelText('Search returned records'),{target:{value:'needle'}});
    fireEvent.click(screen.getByRole('button',{name:'Scan topic'}));
    expect(await screen.findByText('Scanning orders for “needle”…')).toBeVisible();
    fireEvent.click(screen.getByRole('button',{name:'Cancel scan'}));
    expect(signal?.aborted).toBe(true);
    await vi.waitFor(()=>expect(screen.queryByText('Scanning orders for “needle”…')).toBeNull());
    expect(screen.queryByRole('alert')).toBeNull();
    expect(screen.getByText('No returned records match this search.')).toBeVisible();
  });
});
