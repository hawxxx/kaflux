import {useState,type FormEvent} from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import {ArrowRight,Plus,Trash2,X} from 'lucide-react';
import {api,bytes,type Message} from './api';

export type Encoding='text'|'json'|'base64';
export type Header={key:string;value:string};
export type Draft={partition:string;key:string;keyEncoding:Encoding;value:string;valueEncoding:Encoding;headers:Header[]};
export const emptyDraft:Draft={partition:'auto',key:'',keyEncoding:'text',value:'',valueEncoding:'text',headers:[]};
// Mirrors the server bounds so oversized records are refused before the request.
export const limits={key:64*1024,value:512*1024,headers:32};

// Kafka's default partitioner: murmur2 of the key bytes, made positive, modulo the partition count.
export function murmur2(data:Uint8Array){
  const m=0x5bd1e995;let h=0x9747b28c^data.length;const whole=data.length&~3;
  for(let i=0;i<whole;i+=4){let k=data[i]|data[i+1]<<8|data[i+2]<<16|data[i+3]<<24;k=Math.imul(k,m);k^=k>>>24;k=Math.imul(k,m);h=Math.imul(h,m)^k}
  const rest=data.length&3;
  if(rest===3)h^=data[whole+2]<<16;
  if(rest>=2)h^=data[whole+1]<<8;
  if(rest>=1){h^=data[whole];h=Math.imul(h,m)}
  h^=h>>>13;h=Math.imul(h,m);h^=h>>>15;return h|0;
}
export function keyPartition(key:Uint8Array,partitions:number){return (murmur2(key)&0x7fffffff)%partitions}

function base64Bytes(text:string){
  const clean=text.replace(/\s/g,'');
  if(clean.length%4===1||!/^[A-Za-z0-9+/]*={0,2}$/.test(clean))return null;
  try{return Uint8Array.from(atob(clean),c=>c.charCodeAt(0))}catch{return null}
}
const toBase64=(data:Uint8Array)=>btoa(Array.from(data,b=>String.fromCharCode(b)).join(''));
// Encodes one draft field; the server receives base64 for binary input and text otherwise.
export function encodeField(text:string,encoding:Encoding,limit:number,label:string):{data:Uint8Array;error?:string;base64?:string}{
  if(encoding==='base64'){const data=base64Bytes(text);if(!data)return {data:new Uint8Array(),error:`${label} is not valid Base64.`};return {data,base64:data.length?toBase64(data):undefined,error:data.length>limit?`${label} exceeds ${bytes(limit)}.`:undefined}}
  const data=new TextEncoder().encode(text);
  if(encoding==='json'&&text.trim()){try{JSON.parse(text)}catch(e){return {data,error:`${label} is not valid JSON: ${(e as Error).message}`}}}
  return {data,error:data.length>limit?`${label} exceeds ${bytes(limit)}.`:undefined};
}

function decodeBytes(base64:string|undefined,fallback:unknown):{text:string;encoding:Encoding}{
  const data=base64?base64Bytes(base64):null;
  if(base64&&data){try{const text=new TextDecoder('utf-8',{fatal:true}).decode(data);return {text,encoding:isJson(text)?'json':'text'}}catch{return {text:base64,encoding:'base64'}}}
  const text=fallback==null?'':typeof fallback==='string'?fallback:JSON.stringify(fallback,null,2);
  return {text,encoding:isJson(text)?'json':'text'};
}
function isJson(text:string){if(!/^\s*[[{]/.test(text))return false;try{JSON.parse(text);return true}catch{return false}}
// A returned record becomes a draft from its original bytes; binary payloads stay Base64 so nothing is re-encoded.
export function draftFrom(m:Message):Draft{
  const key=decodeBytes(m.keyBase64,m.key),value=decodeBytes(m.valueBase64,m.value);
  return {partition:String(m.partition),key:key.text,keyEncoding:key.encoding,value:value.text,valueEncoding:value.encoding,headers:(m.headers??[]).map(h=>({key:h.key,value:h.value}))};
}

function EncodingSelect({label,value,onChange,json=true}:{label:string;value:Encoding;onChange:(e:Encoding)=>void;json?:boolean}){
  return <div className="segmented" role="group" aria-label={label}>{(json?['text','json','base64']:['text','base64']).map(x=><button type="button" key={x} className={value===x?'selected':''} aria-pressed={value===x} onClick={()=>onChange(x as Encoding)}>{x==='json'?'JSON':x==='base64'?'Base64':'Text'}</button>)}</div>;
}

export function ProduceDialog({open,onOpenChange,clusterId,topic,partitions,draft,onDraft,onProduced}:{open:boolean;onOpenChange:(open:boolean)=>void;clusterId:string;topic:string;partitions?:number;draft:Draft;onDraft:(d:Draft)=>void;onProduced:(m:Message)=>void}){
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const set=(patch:Partial<Draft>)=>{onDraft({...draft,...patch});setError('')};
  const key=encodeField(draft.key,draft.keyEncoding,limits.key,'Key');
  const value=encodeField(draft.value,draft.valueEncoding,limits.value,'Value');
  const headers=draft.headers.filter(h=>h.key||h.value);
  const headerError=headers.some(h=>!h.key)?'Every header needs a name.':headers.length>limits.headers?`At most ${limits.headers} headers.`:undefined;
  const problem=key.error??value.error??headerError;
  const keyed=key.data.length>0;
  const resolved=draft.partition!=='auto'?Number(draft.partition):partitions&&keyed?keyPartition(key.data,partitions):undefined;
  async function submit(e:FormEvent){
    e.preventDefault();
    if(problem||busy||!partitions)return;
    setBusy(true);setError('');
    try{
      const partition=resolved??Math.floor(Math.random()*partitions);
      const body={topic,partition,headers,...(key.base64?{keyBase64:key.base64}:{key:draft.keyEncoding==='base64'?'':draft.key}),...(value.base64?{valueBase64:value.base64}:{value:draft.valueEncoding==='base64'?'':draft.value})};
      const result=await api<Message>(`/clusters/${clusterId}/messages`,{method:'POST',body:JSON.stringify(body)});
      onProduced(result.data);onOpenChange(false);
    }catch(e){setError((e as Error).message)}finally{setBusy(false)}
  }
  function formatJson(){try{set({value:JSON.stringify(JSON.parse(draft.value),null,2)})}catch{/* the inline error already explains it */}}
  const setHeader=(i:number,patch:Partial<Header>)=>set({headers:draft.headers.map((h,j)=>j===i?{...h,...patch}:h)});
  return <Dialog.Root open={open} onOpenChange={o=>{if(!busy)onOpenChange(o)}}>
    <Dialog.Portal>
      <Dialog.Overlay className="dialog-overlay"/>
      <Dialog.Content className="dialog dialog-wide produce-dialog">
        <Dialog.Title>Produce a message</Dialog.Title>
        <Dialog.Description>Review the destination and payload. This action is authorized and audited; payloads are not included in audit events.</Dialog.Description>
        <Dialog.Close className="dialog-close" aria-label="Close"><X size={18}/></Dialog.Close>
        <form onSubmit={submit} noValidate>
          <div className="produce-destination">
            <div><span>Topic</span><strong>{topic}</strong></div>
            <label>Partition
              <select value={draft.partition} onChange={e=>set({partition:e.target.value})} disabled={!partitions}>
                <option value="auto">Auto · {keyed?'by key hash':'random'}</option>
                {Array.from({length:partitions??0},(_,i)=><option key={i} value={i}>Partition {i}</option>)}
              </select>
            </label>
            <small>{!partitions?'Loading partitions…':resolved===undefined?'A random partition is chosen on send.':`Lands on partition ${resolved}${draft.partition==='auto'?', as Kafka\'s default partitioner would place this key':''}.`}</small>
          </div>
          <fieldset className="produce-field">
            <legend>Key <small>{bytes(key.data.length)} · max {bytes(limits.key)}</small></legend>
            <EncodingSelect label="Key encoding" value={draft.keyEncoding} onChange={keyEncoding=>set({keyEncoding})} json={false}/>
            <input aria-label="Message key" value={draft.key} onChange={e=>set({key:e.target.value})} placeholder={draft.keyEncoding==='base64'?'Base64 bytes':'Leave empty for no key'} spellCheck={false}/>
            {key.error&&<p className="field-error">{key.error}</p>}
          </fieldset>
          <fieldset className="produce-field">
            <legend>Value <small>{bytes(value.data.length)} · max {bytes(limits.value)}</small></legend>
            <div className="produce-field-tools">
              <EncodingSelect label="Value encoding" value={draft.valueEncoding} onChange={valueEncoding=>set({valueEncoding})}/>
              {draft.valueEncoding==='json'&&<button type="button" className="link-button" onClick={formatJson} disabled={!!value.error||!draft.value.trim()}>Format</button>}
            </div>
            <textarea aria-label="Message value" value={draft.value} onChange={e=>set({value:e.target.value})} rows={9} spellCheck={false} placeholder={draft.valueEncoding==='json'?'{"type":"example"}':draft.valueEncoding==='base64'?'Base64 bytes':'Message value'}/>
            {value.error&&<p className="field-error">{value.error}</p>}
          </fieldset>
          <fieldset className="produce-field">
            <legend>Headers <small>{headers.length} of {limits.headers}</small></legend>
            {draft.headers.map((h,i)=><div className="header-row" key={i}>
              <input aria-label={`Header ${i+1} name`} placeholder="Name" value={h.key} onChange={e=>setHeader(i,{key:e.target.value})} spellCheck={false}/>
              <input aria-label={`Header ${i+1} value`} placeholder="Value" value={h.value} onChange={e=>setHeader(i,{value:e.target.value})} spellCheck={false}/>
              <button type="button" className="header-remove" aria-label={`Remove header ${i+1}`} onClick={()=>set({headers:draft.headers.filter((_,j)=>j!==i)})}><Trash2 size={14}/></button>
            </div>)}
            <button type="button" className="link-button" disabled={draft.headers.length>=limits.headers} onClick={()=>set({headers:[...draft.headers,{key:'',value:''}]})}><Plus size={13}/>Add header</button>
            {headerError&&<p className="field-error">{headerError}</p>}
          </fieldset>
          {error&&<p className="form-error" role="alert">{error}</p>}
          <div className="produce-actions">
            <button type="button" className="button" onClick={()=>onDraft(emptyDraft)} disabled={busy}>Clear</button>
            <button className="button primary" disabled={!!problem||busy||!partitions} aria-busy={busy}>{busy?'Producing…':'Confirm & produce'}<ArrowRight size={14}/></button>
          </div>
        </form>
      </Dialog.Content>
    </Dialog.Portal>
  </Dialog.Root>;
}
