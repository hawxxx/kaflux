import {useEffect,useRef,useState} from 'react';
import {X} from 'lucide-react';
import {api,type Topic} from './api';

const pageSize=200;
/** Selections larger than this need an explicit yes; they make slow plans and long runs. */
export const confirmAbove=100;
/** Selections larger than this collapse into a summary instead of one chip per topic. */
export const collapseAbove=30;

/** Fetches topic names matching `q`, page by page, so "Add all" works past the picker's 50-row window. Internal topics are left out. */
export async function allTopicNames(clusterId:string,q:string,opts:{signal?:AbortSignal;onProgress?:(loaded:number,total:number)=>void}={}):Promise<string[]>{
  const names:string[]=[];
  for(let page=1;;page++){
    const r=await api<Topic[]>(`/clusters/${clusterId}/topics?q=${encodeURIComponent(q)}&internal=false&page=${page}&pageSize=${pageSize}`,{signal:opts.signal});
    names.push(...r.data.map(t=>t.name));
    const total=r.meta?.total??names.length;
    opts.onProgress?.(Math.min(names.length,total),total);
    if(!r.data.length||names.length>=total)return names;
  }
}

/** Bulk actions for a multi-select topic list: add every (matching) topic, or clear the selection. */
export function TopicBulk({clusterId,selected,onChange,search=''}:{clusterId:string;selected:string[];onChange:(topics:string[])=>void;search?:string}){
  const [progress,setProgress]=useState<{loaded:number;total:number}|null>(null);
  const [pending,setPending]=useState<string[]|null>(null);
  const [error,setError]=useState('');
  const abort=useRef<AbortController|null>(null);
  // Merge against the live selection, not the one captured when the click started a slow load.
  const latest=useRef(selected);latest.current=selected;
  const merge=(names:string[])=>{const cur=latest.current;onChange([...cur,...names.filter(n=>!cur.includes(n))])};
  // A different cluster or search invalidates any load or confirmation in flight.
  useEffect(()=>{setPending(null);setError('');return()=>abort.current?.abort()},[clusterId,search]);
  async function addAll(){
    setError('');setPending(null);
    const controller=new AbortController();abort.current=controller;setProgress({loaded:0,total:0});
    try{
      const names=await allTopicNames(clusterId,search,{signal:controller.signal,onProgress:(loaded,total)=>setProgress({loaded,total})});
      const fresh=names.filter(n=>!latest.current.includes(n));
      if(!fresh.length)setError(search?`No more topics match “${search}”.`:'No more topics to add.');
      else if(fresh.length>confirmAbove)setPending(fresh);
      else merge(fresh);
    }catch(e){if((e as Error).name!=='AbortError')setError((e as Error).message)}
    finally{abort.current=null;setProgress(null)}
  }
  const label=search?`Add all matching “${search}”`:'Add all topics';
  return <span className="topic-bulk">
    {progress?<>
      <span className="topic-bulk-status" role="status">{progress.total?`Loading ${progress.loaded.toLocaleString()} of ${progress.total.toLocaleString()}…`:'Loading…'}</span>
      <button type="button" className="button tint" onClick={()=>abort.current?.abort()}>Cancel</button>
    </>:pending?<span className="topic-bulk-confirm" role="alertdialog" aria-label="Confirm adding topics">
      <span>Add {pending.length.toLocaleString()} topics? Large plans take longer to generate and run.</span>
      <button type="button" className="button primary" onClick={()=>{merge(pending);setPending(null)}}>Add {pending.length.toLocaleString()}</button>
      <button type="button" className="button tint" onClick={()=>setPending(null)}>Cancel</button>
    </span>:<>
      <button type="button" className="button tint" onClick={addAll}>{label}</button>
      <button type="button" className="button tint" disabled={!selected.length} onClick={()=>onChange([])}>Clear{selected.length?` (${selected.length.toLocaleString()})`:''}</button>
    </>}
    {error&&<span role="alert" className="topic-bulk-error">{error}</span>}
  </span>;
}

/** A compact stand-in for the chip row when many topics are selected. */
export function TopicSummary({topics,onRemove}:{topics:string[];onRemove?:(topic:string)=>void}){
  return <details className="topic-summary">
    <summary>{topics.length.toLocaleString()} topics selected · run in listed order</summary>
    <ol aria-label="Run order">{topics.map(t=><li key={t}>{t}{onRemove&&<button type="button" aria-label={`Remove ${t}`} onClick={()=>onRemove(t)}><X size={11}/></button>}</li>)}</ol>
  </details>;
}
