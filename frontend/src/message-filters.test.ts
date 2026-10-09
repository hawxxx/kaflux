import {describe,expect,it} from 'vitest';
import {filterMessages,mergeMessages} from './message-filters';
import type {Message} from './api';
const record:Message={partition:0,offset:0,timestamp:'2026-10-03T00:00:00Z',key:'Order-A',value:'payload',headers:[{key:'trace-id',value:'ABC-123'}]};
describe('bounded message filters',()=>{
  it('searches key, value and headers case-insensitively',()=>{for(const text of ['order','PAYLOAD','abc-123','trace'])expect(filterMessages([record],{text,scope:'all'})).toEqual([record]);expect(filterMessages([record],{text:'missing',scope:'all'})).toEqual([])});
  it('limits the search to the chosen part of the record',()=>{expect(filterMessages([record],{text:'order',scope:'value'})).toEqual([]);expect(filterMessages([record],{text:'payload',scope:'value'})).toEqual([record]);expect(filterMessages([record],{text:'trace',scope:'key'})).toEqual([]);expect(filterMessages([record],{text:'trace',scope:'headers'})).toEqual([record])});
  it('handles tombstones and absent keys without inventing searchable text',()=>{expect(filterMessages([{...record,key:null,value:null,headers:[]}],{text:'null',scope:'all'})).toEqual([])});
  it('matches structured and schema-decoded payloads',()=>{expect(filterMessages([{...record,key:{id:7}}],{text:'"id":7',scope:'key'})).toHaveLength(1);expect(filterMessages([{...record,value:'binary',decodedValue:{customer:'Ada'}}],{text:'ada',scope:'value'})).toHaveLength(1)});
  it('returns every record for a blank search',()=>expect(filterMessages([record],{text:'  ',scope:'key'})).toEqual([record]));
  it('keeps live tail bounded while retaining records across empty polls and deduplicating offsets',()=>{
    expect(mergeMessages([record],[record,{...record,offset:1},{...record,offset:2}],2).map(r=>r.offset)).toEqual([1,2]);
    expect(mergeMessages([record],[],2)).toEqual([record]);
  });
});
