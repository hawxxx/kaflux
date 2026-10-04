import {useState} from 'react';
import {asJson,hex,pretty} from './api';
export function JsonTree({value,name,depth=0}:{value:unknown;name?:string;depth?:number}) {
  const [open,setOpen]=useState(depth<2);
  const [page,setPage]=useState(0);
  if(value!==null&&typeof value==='object'){
    const entries=Object.entries(value);const array=Array.isArray(value);
    if(depth>=24)return <p role="status">Nesting preview limit reached. Inspect the original payload in Raw.</p>;
    const currentPage=Math.min(page,Math.max(0,Math.ceil(entries.length/50)-1));const start=currentPage*50;
    return <div className="json-branch"><button className="tree-toggle" onClick={()=>setOpen(!open)} aria-expanded={open}>{open?'▾':'▸'} {name&&<span className="json-key">{name}: </span>}<span className="json-bracket">{array?'[':'{'} {!open&&`${entries.length} ${array?'items':'fields'}`} {!open&&(array?']':'}')}</span></button>{open&&<><div className="json-children">{entries.slice(start,start+50).map(([key,item])=><JsonTree key={key} name={key} value={item} depth={depth+1}/>)}</div>{entries.length>50&&<div className="payload-toolbar"><button aria-label="Previous fields" disabled={currentPage===0} onClick={()=>setPage(currentPage-1)}>Previous</button><span>{start+1}–{Math.min(start+50,entries.length)} of {entries.length}</span><button aria-label="Next fields" disabled={start+50>=entries.length} onClick={()=>setPage(currentPage+1)}>Next</button></div>}<span className="json-bracket">{array?']':'}'}</span></>}</div>
  }
  return <div className="json-leaf">{name&&<span className="json-key">{name}: </span>}<span className={typeof value==='string'?'json-string':typeof value==='number'?'json-number':'json-literal'}>{JSON.stringify(value)}</span></div>
}
export function MessageValue({value,bytesBase64,decodedValue,schemaId,decodeError,decodedFormat}:{value:unknown;bytesBase64?:string;decodedValue?:unknown;schemaId?:number;decodeError?:string;decodedFormat?:string}){
  const [mode,setMode]=useState('JSON');const [copied,setCopied]=useState(false);
  async function copy(){await navigator.clipboard.writeText(pretty(value));setCopied(true);setTimeout(()=>setCopied(false),1600)}
  function download(){const url=URL.createObjectURL(new Blob([pretty(value)],{type:'application/json'}));const a=document.createElement('a');a.href=url;a.download='kaflux-message.json';a.click();URL.revokeObjectURL(url)}
  let hexValue=hex(value);
  if(bytesBase64){try{hexValue=Array.from(atob(bytesBase64)).map(char=>char.charCodeAt(0).toString(16).padStart(2,'0')).join(' ')}catch{hexValue='Invalid binary encoding returned by the server.'}}
  const base64Value=bytesBase64??btoa(Array.from(new TextEncoder().encode(typeof value==='string'?value:JSON.stringify(value)??'')).map(byte=>String.fromCharCode(byte)).join(''));
  return <div className="payload"><div className="payload-toolbar"><div className="segmented">{['JSON','Raw','Text','Hex','Base64','Schema'].map(x=><button key={x} className={mode===x?'selected':''} onClick={()=>setMode(x)}>{x}</button>)}</div><div><button onClick={copy}>{copied?'Copied':'Copy'}</button><button onClick={download}>Download</button></div></div><div className="payload-code">{mode==='Schema'?decodeError?<p className="unsupported-message" role="status">{decodeError}. Raw bytes remain available in Hex and Base64.</p>:decodedValue!==undefined?<><p className="muted">{decodedFormat==='protobuf'?'Protobuf':'Avro'} · schema {schemaId}</p><JsonTree value={decodedValue}/></>:<p className="unsupported-message" role="status">Schema decoding is unavailable. Select Avro or Protobuf and a configured schema registry in the message controls. Raw bytes remain available in Hex and Base64.</p>:mode==='JSON'?<JsonTree value={asJson(value)}/>:<pre>{mode==='Base64'?base64Value:mode==='Hex'?hexValue:mode==='Raw'?typeof value==='string'?value:JSON.stringify(value):pretty(value)}</pre>}</div></div>
}
