import type {Message} from './api';
export function mergeMessages(previous:Message[],incoming:Message[],limit:number){
  const merged=new Map<string,Message>();
  for(const record of [...previous,...incoming])merged.set(`${record.partition}:${record.offset}`,record);
  return Array.from(merged.values()).slice(-Math.max(1,limit));
}
function searchable(value:unknown){if(value==null)return '';return (typeof value==='string'?value:JSON.stringify(value)??'').toLowerCase()}
export function filterMessages(records:Message[],filters:{key:string;header:string}){
  const key=filters.key.trim().toLowerCase(),header=filters.header.trim().toLowerCase();
  return records.filter(record=>(!key||searchable(record.key).includes(key))&&(!header||(record.headers??[]).some(h=>searchable(h.key).includes(header)||searchable(h.value).includes(header))));
}
