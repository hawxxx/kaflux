import {fireEvent,render,screen,waitFor,within} from '@testing-library/react';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {useState} from 'react';
import {ProduceDialog,draftFrom,emptyDraft,encodeField,keyPartition,murmur2,type Draft} from './ProduceDialog';
import type {Message} from './api';

const utf8=(s:string)=>new TextEncoder().encode(s);
describe('key partitioning',()=>{
  // Reference values from Kafka's UtilsTest.testMurmur2.
  it('matches Kafka murmur2',()=>{
    const cases:[string,number][]=[['21',-973932308],['foobar',-790332482],['a-little-bit-long-string',-985981536],['a-little-bit-longer-string',-1486304829],['lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8',-58897971],['abc',479470107]];
    for(const [input,hash] of cases)expect(murmur2(utf8(input))).toBe(hash);
  });
  it('places keys like the default partitioner',()=>{expect(keyPartition(utf8('foobar'),6)).toBe((-790332482&0x7fffffff)%6)});
});
describe('payload encoding',()=>{
  it('measures UTF-8 bytes and enforces limits',()=>{expect(encodeField('é','text',10,'Value').data.length).toBe(2);expect(encodeField('abcd','text',3,'Value').error).toMatch('exceeds')});
  it('validates JSON only when asked',()=>{expect(encodeField('{bad','json',100,'Value').error).toMatch('not valid JSON');expect(encodeField('{bad','text',100,'Value').error).toBeUndefined()});
  it('decodes Base64 to the exact bytes and rejects malformed input',()=>{const out=encodeField('AP+A','base64',100,'Value');expect(Array.from(out.data)).toEqual([0,255,128]);expect(out.base64).toBe('AP+A');expect(encodeField('***','base64',100,'Value').error).toMatch('Base64')});
});
describe('copying a returned record',()=>{
  const record:Message={partition:2,offset:9,timestamp:'2026-10-03T00:00:00Z',key:'k',value:'{"a":1}',keyBase64:btoa('k'),valueBase64:btoa('{"a":1}'),headers:[{key:'trace',value:'x'}]};
  it('keeps text, recognizes JSON and carries headers and partition',()=>expect(draftFrom(record)).toEqual({partition:'2',key:'k',keyEncoding:'text',value:'{"a":1}',valueEncoding:'json',headers:[{key:'trace',value:'x'}]}));
  it('keeps binary values as Base64 instead of re-encoding them',()=>{const d=draftFrom({...record,value:'�',valueBase64:'AP+A'});expect(d.valueEncoding).toBe('base64');expect(d.value).toBe('AP+A')});
});

function Harness({initial=emptyDraft,onProduced=()=>{}}:{initial?:Draft;onProduced?:(m:Message)=>void}){
  const [draft,setDraft]=useState(initial);
  return <ProduceDialog open onOpenChange={()=>{}} clusterId="demo" topic="orders" partitions={4} draft={draft} onDraft={setDraft} onProduced={onProduced}/>;
}
afterEach(()=>vi.unstubAllGlobals());
describe('produce dialog',()=>{
  it('sends the resolved key partition, headers and Base64 value',async()=>{
    const fetch=vi.fn(async(_url:string,init:RequestInit)=>new Response(JSON.stringify({data:{...JSON.parse(String(init.body)),offset:41,timestamp:'2026-10-03T00:00:00Z'}}),{status:200}));
    vi.stubGlobal('fetch',fetch);
    const produced=vi.fn();
    render(<Harness onProduced={produced}/>);
    fireEvent.change(screen.getByLabelText('Message key'),{target:{value:'foobar'}});
    expect(screen.getByText(/Lands on partition 2/)).toBeVisible();
    fireEvent.click(within(screen.getByRole('group',{name:'Value encoding'})).getByRole('button',{name:'Base64'}));
    fireEvent.change(screen.getByLabelText('Message value'),{target:{value:'AP+A'}});
    fireEvent.click(screen.getByRole('button',{name:'Add header'}));
    fireEvent.change(screen.getByLabelText('Header 1 name'),{target:{value:'trace-id'}});
    fireEvent.change(screen.getByLabelText('Header 1 value'),{target:{value:'abc'}});
    fireEvent.click(screen.getByRole('button',{name:'Confirm & produce'}));
    await waitFor(()=>expect(produced).toHaveBeenCalled());
    expect(JSON.parse(String(fetch.mock.calls[0][1].body))).toEqual({topic:'orders',partition:2,key:'foobar',valueBase64:'AP+A',headers:[{key:'trace-id',value:'abc'}]});
    expect(produced.mock.calls[0][0].offset).toBe(41);
  });
  it('blocks invalid input and shows server failures inline',async()=>{
    vi.stubGlobal('fetch',vi.fn(async()=>new Response(JSON.stringify({error:{code:'produce_uncertain',message:'Production outcome uncertain; inspect offsets before retrying'}}),{status:503})));
    render(<Harness initial={{...emptyDraft,valueEncoding:'json',value:'{bad'}}/>);
    expect(screen.getByText(/not valid JSON/)).toBeVisible();
    expect(screen.getByRole('button',{name:'Confirm & produce'})).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Message value'),{target:{value:'{"ok":true}'}});
    fireEvent.click(screen.getByRole('button',{name:'Confirm & produce'}));
    expect(await screen.findByRole('alert')).toHaveTextContent('inspect offsets before retrying');
  });
});
