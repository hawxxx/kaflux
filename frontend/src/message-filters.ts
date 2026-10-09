import type {Message} from './api';
// Records from different partitions interleave by time; offsets only order records within one partition.
export const chronological=(a:Message,b:Message)=>Date.parse(a.timestamp)-Date.parse(b.timestamp)||a.partition-b.partition||a.offset-b.offset;
export function mergeMessages(previous:Message[],incoming:Message[],limit:number,byTime=false){
  const merged=new Map<string,Message>();
  for(const record of [...previous,...incoming])merged.set(`${record.partition}:${record.offset}`,record);
  const all=Array.from(merged.values());
  return (byTime?all.sort(chronological):all).slice(-Math.max(1,limit));
}
export type SearchScope='all'|'key'|'value'|'headers';
function searchable(value:unknown){if(value==null)return '';return (typeof value==='string'?value:JSON.stringify(value)??'').toLowerCase()}
export function filterMessages(records:Message[],{text,scope}:{text:string;scope:SearchScope}){
  const needle=text.trim().toLowerCase();
  if(!needle)return records;
  return records.filter(r=>{
    const fields={key:[r.key,r.decodedKey],value:[r.value,r.decodedValue],headers:(r.headers??[]).flatMap(h=>[h.key,h.value])};
    return (scope==='all'?Object.values(fields).flat():fields[scope]).some(v=>searchable(v).includes(needle));
  });
}
