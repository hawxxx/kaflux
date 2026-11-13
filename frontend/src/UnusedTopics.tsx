import {useEffect,useMemo,useRef,useState} from 'react';
import {keepPreviousData,useQuery,useQueryClient} from '@tanstack/react-query';
import {ArrowDownToLine,ArrowRight,Minus,Plus,RefreshCw,Shield,Trash2} from 'lucide-react';
import {api,bytes,can,type Session} from './api';
import {Modal} from './Administration';

type Unused={name:string;classification:'unused'|'likely-unused'|'idle';reasons:string[];partitions:number;emptyPartitions:number[];records:number;sizeBytes:number|null;lastRecordAt:string|null;consumerGroups:string[];activeConsumers:number;internal:boolean;ageKnown:boolean;fingerprint:string};
type Report={topics:Unused[];evaluatedAt:string;partial:boolean;partialReason?:string};
type BulkResult={deleted:string[];skipped:{topic:string;reason:string}[]};
const kinds=[['all','All'],['unused','Unused'],['likely-unused','Likely unused'],['idle','Idle']] as const;
const MAX_BULK=100;
const sorts=[['name','Name'],['size','Largest first'],['oldest','Oldest write first']] as const;
export function validRegex(pattern:string){if(pattern.length>200)return false;try{new RegExp(pattern);return true}catch{return false}}
export function ageLabel(iso:string|null,now=Date.now()){if(!iso)return '';const days=Math.floor((now-new Date(iso).getTime())/86400000);return days<1?'today':days===1?'1 day ago':days<60?`${days} days ago`:`${Math.floor(days/30)} months ago`}
const csvCell=(v:unknown)=>{const s=String(v??'');return /[",\n]/.test(s)?`"${s.replace(/"/g,'""')}"`:s};
export function unusedCsv(rows:Unused[]){
  const head=['topic','classification','reasons','partitions','emptyPartitions','records','sizeBytes','lastRecordAt','consumerGroups'];
  return [head,...rows.map(r=>[r.name,r.classification,r.reasons.join('; '),r.partitions,r.emptyPartitions.join(' '),r.records<0?'':r.records,r.sizeBytes??'',r.lastRecordAt??'',r.consumerGroups.join(' ')])].map(l=>l.map(csvCell).join(',')).join('\n')+'\n';
}
function downloadCsv(rows:Unused[]){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([unusedCsv(rows)],{type:'text/csv'}));a.download='unused-topics.csv';a.click();setTimeout(()=>URL.revokeObjectURL(a.href),100)}
/** Whole-number field with the console's −/+ stepper (the throttle stepper style); no native spinner. */
function DaysStepper({value,onChange}:{value:string;onChange:(v:string)=>void}){
  const bump=(by:number)=>onChange(String(Math.min(3650,Math.max(1,(Number(value)||0)+by))));
  return <div className="throttle-stepper">
    <button type="button" aria-label="Decrease days" disabled={Number(value)<=1} onClick={()=>bump(-1)}><Minus size={13} aria-hidden/></button>
    <input inputMode="numeric" aria-label="Unused for at least (days)" value={value} onChange={e=>onChange(e.target.value.replace(/[^0-9]/g,''))}/>
    <button type="button" aria-label="Increase days" disabled={Number(value)>=3650} onClick={()=>bump(1)}><Plus size={13} aria-hidden/></button>
  </div>;
}
/** Finds topics nobody reads or writes and deletes the chosen ones safely: the server re-checks each at delete time. */
export function UnusedTopics({clusterId,session}:{clusterId:string;session?:Session}){
  const [days,setDays]=useState('30');const [internal,setInternal]=useState(false);const [exclude,setExclude]=useState('');
  const [filter,setFilter]=useState('');const [sort,setSort]=useState<typeof sorts[number][0]>('name');const [dropped,setDropped]=useState(0);
  const [kind,setKind]=useState<typeof kinds[number][0]>('all');const [selected,setSelected]=useState<string[]>([]);
  const [open,setOpen]=useState(false);const [confirmation,setConfirmation]=useState('');const [busy,setBusy]=useState(false);const [error,setError]=useState('');const [outcome,setOutcome]=useState<BulkResult|null>(null);const [emptyUnknown,setEmptyUnknown]=useState(false);
  const queryClient=useQueryClient();
  const n=Number(days);const validDays=Number.isInteger(n)&&n>=1&&n<=3650;
  const [debounced,setDebounced]=useState('');
  useEffect(()=>{const t=setTimeout(()=>setDebounced(exclude.trim()),350);return()=>clearTimeout(t)},[exclude]);
  const excludeValid=validRegex(exclude);
  const effectiveExclude=excludeValid?debounced:'';
  const query=useQuery({queryKey:['unused-topics',clusterId,n,internal,effectiveExclude],enabled:validDays,retry:false,placeholderData:keepPreviousData,queryFn:async()=>(await api<Report>(`/clusters/${clusterId}/unused-topics?${new URLSearchParams({minAgeDays:String(n),includeInternal:String(internal),exclude:effectiveExclude})}`)).data});
  const rows=query.data?.topics??[];
  const needle=filter.trim().toLowerCase();
  const shown=useMemo(()=>{const out=rows.filter(r=>(kind==='all'||r.classification===kind)&&(!needle||r.name.toLowerCase().includes(needle)));
    const time=(r:Unused)=>r.lastRecordAt?new Date(r.lastRecordAt).getTime():Infinity;
    return sort==='size'?[...out].sort((a,b)=>(b.sizeBytes??-1)-(a.sizeBytes??-1)||a.name.localeCompare(b.name)):sort==='oldest'?[...out].sort((a,b)=>time(a)-time(b)||a.name.localeCompare(b.name)):out},[rows,kind,needle,sort]);
  const names=rows.map(r=>r.name).join('\n');
  useEffect(()=>{const live=new Set(names.split('\n'));setSelected(s=>{const kept=s.filter(x=>live.has(x));if(kept.length!==s.length)setDropped(s.length-kept.length);return kept})},[names]);
  const count=(k:string)=>k==='all'?rows.length:rows.filter(r=>r.classification===k).length;
  const deletable=(r:Unused)=>!r.internal;
  const canDelete=can(session,clusterId,'delete');
  const phrase=`delete ${selected.length} topics`;
  const toggle=(name:string)=>{setDropped(0);setSelected(s=>s.includes(name)?s.filter(x=>x!==name):s.length>=MAX_BULK?s:[...s,name])};
  const selectKind=(k:string)=>{setDropped(0);setSelected(rows.filter(r=>deletable(r)&&(k==='all'||r.classification===k)&&(!needle||r.name.toLowerCase().includes(needle))).map(r=>r.name).slice(0,MAX_BULK))};
  const shownDeletable=shown.filter(deletable);
  const shownSelected=shownDeletable.filter(r=>selected.includes(r.name)).length;
  const headRef=useRef<HTMLInputElement>(null);
  useEffect(()=>{if(headRef.current)headRef.current.indeterminate=shownSelected>0&&shownSelected<shownDeletable.length});
  const toggleShown=(on:boolean)=>{setDropped(0);const ids=new Set(shownDeletable.map(r=>r.name));setSelected(s=>on?[...new Set([...s,...shownDeletable.map(r=>r.name)])].slice(0,MAX_BULK):s.filter(x=>!ids.has(x)))};
  const selectedBytes=selected.reduce((sum,t)=>sum+(rows.find(r=>r.name===t)?.sizeBytes??0),0);
  function show(){setOpen(true);setConfirmation('');setError('');setOutcome(null);setEmptyUnknown(false)}
  async function submit(e:React.FormEvent){e.preventDefault();setBusy(true);setError('');
    try{const out=(await api<BulkResult>(`/clusters/${clusterId}/topics/bulk-delete`,{method:'POST',body:JSON.stringify({topics:selected,minAgeDays:n,confirmation,fingerprints:Object.fromEntries(selected.map(t=>[t,rows.find(r=>r.name===t)?.fingerprint??''])),includeEmptyUnknownAge:emptyUnknown})})).data;
      setOutcome(out);setSelected(s=>s.filter(x=>!out.deleted.includes(x)));await queryClient.invalidateQueries()}
    catch(err){setError((err as Error).message)}finally{setBusy(false)}}
  return <section className="panel">
    <div className="panel-title"><div><h2>Topic cleanup</h2><p>Topics with no consumers and no recent writes. Review the reasons, then delete in bulk.</p></div></div>
    <div className="panel-body unused-controls">
      <div className="field"><span>Unused for at least (days)</span><DaysStepper value={days} onChange={setDays}/></div>
      <label>Exclude names matching (regex)<input value={exclude} onChange={e=>setExclude(e.target.value)} maxLength={200} autoComplete="off" placeholder="^(tmp|test)\." aria-invalid={!excludeValid} aria-describedby={excludeValid?undefined:'exclude-error'}/>{!excludeValid&&<small id="exclude-error" className="bad-text" role="alert">Not a valid regular expression; the last valid filter is still applied.</small>}</label>
      <label className="check-row"><input type="checkbox" checked={internal} onChange={e=>setInternal(e.target.checked)}/> Show internal topics (listed for review only, never deletable)</label>
    </div>
    <div className="unused-status" role="status" aria-live="polite"><span className="muted">{query.isFetching?'Evaluating topics…':query.data?`Evaluated ${new Date(query.data.evaluatedAt).toLocaleTimeString()} · ${rows.length} candidate${rows.length===1?'':'s'}`:''}{selected.length>0&&<span> · {selected.length} selected, {bytes(selectedBytes)} reclaimable</span>}</span><button type="button" className="button" onClick={()=>query.refetch()} disabled={!validDays||query.isFetching}><RefreshCw size={14} className={query.isFetching?'spin':undefined}/>Refresh</button></div>
    {!validDays&&<div className="inline-error" role="alert">Days must be a whole number between 1 and 3650.</div>}
    {internal&&<div className="mutation-warning"><Shield size={16}/><p>Internal topics are listed for review only. Kaflux never deletes them from this page.</p></div>}
    {query.error&&<div className="inline-error" role="alert">{query.error.message} <button type="button" className="link-button" onClick={()=>query.refetch()}>Retry</button></div>}
    {dropped>0&&<div className="inline-notice" role="status">{dropped} selected topic{dropped===1?' was':'s were'} removed from the selection: {dropped===1?'it no longer qualifies':'they no longer qualify'} as unused.</div>}
    {selected.length>=MAX_BULK&&<div className="inline-notice" role="status">Selection limit reached: at most {MAX_BULK} topics can be deleted at once.</div>}
    {query.data?.partial&&<div className="inline-notice" role="status">Partial result: {query.data.partialReason||'some topics could not be evaluated'}. Topics without complete evidence are left out and cannot be deleted.</div>}
    <div className="table-toolbar"><div className="segmented" role="group" aria-label="Classification">{kinds.map(([k,label])=><button key={k} type="button" aria-pressed={kind===k} className={kind===k?'active':''} onClick={()=>setKind(k)}>{label} ({count(k)})</button>)}</div>
      <div className="toolbar-right">
        <input type="search" className="unused-search" aria-label="Filter topics by name" placeholder="Filter by name…" value={filter} onChange={e=>setFilter(e.target.value)} maxLength={200}/>
        <select aria-label="Sort topics" value={sort} onChange={e=>setSort(e.target.value as typeof sort)}>{sorts.map(([k,label])=><option key={k} value={k}>{label}</option>)}</select>
        {kinds.slice(1).map(([k,label])=><button key={k} type="button" className="button" disabled={count(k)===0} onClick={()=>selectKind(k)}>Select all {label.toLowerCase()}</button>)}
        <button type="button" className="button" disabled={rows.length===0} onClick={()=>downloadCsv(shown)}><ArrowDownToLine size={14}/>Download CSV</button>
        <button type="button" className="button danger-button" disabled={!canDelete||selected.length===0} onClick={show}><Trash2 size={14}/>Delete selected ({selected.length})</button>
      </div></div>
    {!canDelete&&<p className="muted">Deleting needs the delete permission.</p>}
    {query.isLoading?<p className="muted" role="status">Evaluating topics…</p>:<div className="table-scroll" aria-busy={query.isFetching}><table aria-label="Unused topics"><thead><tr><th><input ref={headRef} type="checkbox" aria-label="Select all shown" disabled={shownDeletable.length===0} checked={shownDeletable.length>0&&shownSelected===shownDeletable.length} onChange={e=>toggleShown(e.target.checked)}/></th><th>Topic</th><th>Reasons</th><th>Records</th><th>Size</th><th>Last write</th><th>Groups</th></tr></thead>
      <tbody>{shown.map(r=><tr key={r.name}>
        <td><input type="checkbox" aria-label={`Select ${r.name}`} disabled={!deletable(r)||(!selected.includes(r.name)&&selected.length>=MAX_BULK)} checked={selected.includes(r.name)} onChange={()=>toggle(r.name)}/></td>
        <td><span className="mono">{r.name}</span> <span className="tag">{r.classification}</span>{r.internal&&<span className="tag tag-internal">INTERNAL</span>}{!r.ageKnown&&<span className="tag" title="The topic has no records, so nothing shows how old it is.">age unknown — may be brand new</span>}{r.emptyPartitions.length>0&&<span className="tag" title={`Partitions ${r.emptyPartitions.join(', ')} hold no records. Kafka cannot delete partitions: recreate the topic with fewer partitions instead.`}>{r.emptyPartitions.length} empty partition{r.emptyPartitions.length===1?'':'s'}</span>}</td>
        <td>{r.reasons.join('; ')}</td><td>{r.records<0?'Unknown':r.records.toLocaleString()}</td><td>{bytes(r.sizeBytes)}</td><td>{r.lastRecordAt?<>{new Date(r.lastRecordAt).toLocaleString()}<br/><small className="muted">{ageLabel(r.lastRecordAt)}</small></>:'Unknown'}</td><td>{r.consumerGroups.length?r.consumerGroups.join(', '):'None'}</td></tr>)}
        {shown.length===0&&!query.error&&<tr><td colSpan={7} className="muted">{rows.length===0?'No unused topics found for these settings.':'No topics match the current filters.'}</td></tr>}</tbody></table></div>}
    <Modal open={open} onClose={()=>{if(!busy)setOpen(false)}} title={`Delete ${selected.length} topics`} description="Each topic is re-checked on the server just before deletion; topics now in use are skipped." wide><form onSubmit={submit}>
      <ul className="bulk-topic-list">{(outcome?[...outcome.deleted,...outcome.skipped.map(s=>s.topic)]:selected).map(t=>{const row=rows.find(r=>r.name===t);const skip=outcome?.skipped.find(s=>s.topic===t);return <li key={t}><span className="mono">{t}</span>{row&&<span className="muted"> {row.reasons.join('; ')}</span>}{outcome&&(skip?<span className="bad-text"> Skipped: {skip.reason}</span>:<span className="good-text"> Deleted</span>)}</li>})}</ul>
      <div className="mutation-warning"><Trash2 size={16}/><p>Deletion removes these topics and their records. This cannot be rolled back through Kaflux.</p></div>
      {error&&<div className="inline-error" role="alert">{error}</div>}
      {outcome&&<div className="inline-notice" role="status">{outcome.deleted.length} deleted, {outcome.skipped.length} skipped.</div>}
      {!outcome&&selected.some(t=>rows.find(r=>r.name===t&&!r.ageKnown))&&<label className="check-row"><input type="checkbox" checked={emptyUnknown} onChange={e=>setEmptyUnknown(e.target.checked)}/> Also delete empty topics whose age is unknown (they may be newly created)</label>}
      {!outcome&&<><label>Type <strong className="mono">{phrase}</strong> to confirm<input autoFocus value={confirmation} onChange={e=>setConfirmation(e.target.value)} required autoComplete="off"/></label>
      <button className="button danger-button" disabled={busy||confirmation!==phrase}>{busy?'Deleting…':'Confirm deletion'}<ArrowRight size={14}/></button></>}
      {outcome&&<button type="button" className="button" onClick={()=>setOpen(false)}>Close</button>}
    </form></Modal>
  </section>
}
