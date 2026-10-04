import {describe,expect,it} from 'vitest';
import {filterMessages,mergeMessages} from './message-filters';
import type {Message} from './api';
const record:Message={partition:0,offset:0,timestamp:'2026-10-03T00:00:00Z',key:'Order-A',value:'payload',headers:[{key:'trace-id',value:'ABC-123'}]};
describe('bounded message filters',()=>{
  it('matches key and header together, case-insensitively',()=>{expect(filterMessages([record],{key:'order',header:'abc'})).toEqual([record]);expect(filterMessages([record],{key:'order',header:'missing'})).toEqual([])});
  it('handles tombstones and absent keys without inventing searchable text',()=>{expect(filterMessages([{...record,key:null,value:null,headers:[]}],{key:'null',header:''})).toEqual([])});
  it('matches structured keys and header names',()=>{expect(filterMessages([{...record,key:{id:7}}],{key:'"id":7',header:'trace'})).toHaveLength(1)});
  it('keeps live tail bounded while retaining records across empty polls and deduplicating offsets',()=>{
    expect(mergeMessages([record],[record,{...record,offset:1},{...record,offset:2}],2).map(r=>r.offset)).toEqual([1,2]);
    expect(mergeMessages([record],[],2)).toEqual([record]);
  });
});
