import {useState} from 'react';
import {useQuery} from '@tanstack/react-query';
import {api,asJson,pretty} from './api';
import {schemaDiff,type DiffLine} from './schema-diff';
type Schema={schema:string;schemaType?:string;references?:unknown[]};
export function SchemaComparison({resource,versions}:{resource:string;versions:number[]}){
  const [oldVersion,setOldVersion]=useState('');const [newVersion,setNewVersion]=useState('');const before=oldVersion||String(versions[0]??'');const after=newVersion||String(versions.at(-1)??'');
  const first=useQuery({queryKey:[resource,'comparison',before],queryFn:()=>api<Schema>(`${resource}/versions/${before}`),enabled:!!before});
  const second=useQuery({queryKey:[resource,'comparison',after],queryFn:()=>api<Schema>(`${resource}/versions/${after}`),enabled:!!after});
  let lines:DiffLine[]=[];let error='';
  if(first.data&&second.data){try{const normalize=(value:Schema)=>pretty({schemaType:value.schemaType??'AVRO',schema:asJson(value.schema),references:value.references??[]});lines=schemaDiff(normalize(first.data.data),normalize(second.data.data))}catch(e){error=(e as Error).message}}
  return <div className="schema-comparison"><h3>Compare schema versions</h3><p className="muted">Schema text and references from the registry. This comparison does not establish compatibility.</p><div className="comparison-controls"><label>Before version<select value={before} onChange={e=>setOldVersion(e.target.value)}>{versions.map(v=><option key={v}>{v}</option>)}</select></label><label>After version<select value={after} onChange={e=>setNewVersion(e.target.value)}>{versions.map(v=><option key={v}>{v}</option>)}</select></label></div>{(first.error||second.error)&&<div className="inline-error" role="alert">{(first.error??second.error)?.message}</div>}{error&&<div className="inline-error" role="alert">{error}</div>}{first.isLoading||second.isLoading?<p className="empty">Retrieving schema versions…</p>:!error&&first.data&&second.data&&<><div className="diff-summary"><span>{lines.filter(l=>l.kind==='added').length} added lines</span><span>{lines.filter(l=>l.kind==='removed').length} removed lines</span></div><pre className="schema-diff">{lines.map((line,i)=><span className={`diff-${line.kind}`} key={i}>{line.kind==='added'?'+':line.kind==='removed'?'-':' '} {line.line}{'\n'}</span>)}</pre></>}</div>
}
