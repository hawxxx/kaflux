import {useState} from 'react';
import {useQueryClient} from '@tanstack/react-query';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import {ArrowRight,Copy,Eraser,MessageSquare,MoreHorizontal,Plus,RefreshCw,Shield,Trash2,X} from 'lucide-react';
import {api,can,type Session,type Topic} from './api';
import {Modal} from './Administration';
import {useTopicConfig} from './TopicConfiguration';

type Action=''|'copy'|'clear'|'recreate'|'delete';
const topicPath=(clusterId:string,topic:string)=>`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}`;
/** Kafka only accepts DeleteRecords on topics whose cleanup policy includes delete. */
export const clearable=(policy?:string)=>!!policy?.split(',').includes('delete');

const confirmations:Record<'clear'|'recreate'|'delete',{title:string;warning:string;button:string;path:string;method:string}>={
  clear:{title:'Clear messages',warning:'Every record currently in the topic is deleted. The topic, its configuration and consumer offsets stay in place. This cannot be rolled back.',button:'Confirm clearing',path:'/truncate',method:'POST'},
  recreate:{title:'Recreate topic',warning:'The topic is deleted with all its records and created again with the same partition count, replication factor and configuration overrides. Producers and consumers may see errors while it is recreated.',button:'Confirm recreation',path:'/recreate',method:'POST'},
  delete:{title:'Delete topic',warning:'Deletion removes this topic and its records. This action cannot be rolled back through Kaflux.',button:'Confirm deletion',path:'',method:'DELETE'},
};

/** Per-row "⋯" menu on the topic list. Rendered inside a clickable row, so clicks never reach it. */
export function TopicRowMenu({clusterId,topic,session,onBrowse,onCreated}:{clusterId:string;topic:Topic;session?:Session;onBrowse:()=>void;onCreated:(name:string)=>void}){
  const [action,setAction]=useState<Action>('');const [notice,setNotice]=useState('');
  const canCopy=can(session,clusterId,'create'),canDelete=can(session,clusterId,'delete');
  const done=(message:string)=>{setAction('');setNotice(message)};
  return <span className="row-actions" onClick={e=>e.stopPropagation()} onKeyDown={e=>e.stopPropagation()}>
    <DropdownMenu.Root modal={false}>
      <DropdownMenu.Trigger className="icon-button row-menu-trigger" aria-label={`Actions for ${topic.name}`}><MoreHorizontal size={16}/></DropdownMenu.Trigger>
      <DropdownMenu.Portal><DropdownMenu.Content className="row-menu" align="end" sideOffset={4}>
        <DropdownMenu.Item className="row-menu-item" onSelect={onBrowse}><MessageSquare size={14}/>Browse messages</DropdownMenu.Item>
        <DropdownMenu.Item className="row-menu-item" disabled={!canCopy} onSelect={()=>setAction('copy')}><Copy size={14}/>Copy topic…</DropdownMenu.Item>
        <DropdownMenu.Separator className="row-menu-separator"/>
        <DropdownMenu.Item className="row-menu-item" disabled={!canDelete||!clearable(topic.cleanupPolicy)} onSelect={()=>setAction('clear')}><Eraser size={14}/><span>Clear messages{!clearable(topic.cleanupPolicy)&&<small>Requires delete cleanup policy</small>}</span></DropdownMenu.Item>
        <DropdownMenu.Item className="row-menu-item" disabled={!canDelete||!!topic.internal} onSelect={()=>setAction('recreate')}><RefreshCw size={14}/><span>Recreate topic{topic.internal&&<small>Not available for internal topics</small>}</span></DropdownMenu.Item>
        <DropdownMenu.Item className="row-menu-item danger" disabled={!canDelete} onSelect={()=>setAction('delete')}><Trash2 size={14}/>Delete topic</DropdownMenu.Item>
      </DropdownMenu.Content></DropdownMenu.Portal>
    </DropdownMenu.Root>
    {action==='copy'&&<CopyTopic clusterId={clusterId} source={topic} onClose={()=>setAction('')} onCreated={name=>{done(`Created ${name} from ${topic.name}.`);onCreated(name)}}/>}
    {(action==='clear'||action==='recreate'||action==='delete')&&<ConfirmByName clusterId={clusterId} topic={topic.name} kind={action} onClose={()=>setAction('')} onDone={()=>done(`${confirmations[action].title} completed for ${topic.name}.`)}/>}
    {notice&&<div className="toast" role="status" onClick={()=>setNotice('')}>{notice}<X size={14}/></div>}
  </span>
}

function ConfirmByName({clusterId,topic,kind,onClose,onDone}:{clusterId:string;topic:string;kind:'clear'|'recreate'|'delete';onClose:()=>void;onDone:()=>void}){
  const c=confirmations[kind];const queryClient=useQueryClient();
  const [confirmation,setConfirmation]=useState('');const [busy,setBusy]=useState(false);const [error,setError]=useState('');
  async function submit(e:React.FormEvent){e.preventDefault();setBusy(true);setError('');try{
    await api(topicPath(clusterId,topic)+c.path,{method:c.method,body:JSON.stringify({confirmation})});
    await queryClient.invalidateQueries();onDone();
  }catch(err){setError((err as Error).message)}finally{setBusy(false)}}
  return <Modal open onClose={onClose} title={c.title} description="Review the exact change. Backend authorization and audit logging apply to every request."><form onSubmit={submit}><div className="review-destination">Resource<strong>{topic}</strong></div><div className="mutation-warning">{kind==='recreate'?<RefreshCw size={16}/>:<Trash2 size={16}/>}<p>{c.warning}</p></div><label>Type the topic name to confirm<input autoFocus value={confirmation} onChange={e=>setConfirmation(e.target.value)} required autoComplete="off"/></label>{error&&<div className="inline-error" role="alert">{error}</div>}<button className="button danger-button" disabled={busy||confirmation!==topic}>{busy?'Submitting…':c.button}<ArrowRight size={14}/></button></form></Modal>
}

/** Creates a new topic from a source topic's layout and overrides; records are not copied. */
function CopyTopic({clusterId,source,onClose,onCreated}:{clusterId:string;source:Topic;onClose:()=>void;onCreated:(name:string)=>void}){
  const config=useTopicConfig(clusterId,source.name);const queryClient=useQueryClient();
  const [name,setName]=useState(`${source.name}-copy`);const [partitions,setPartitions]=useState(String(source.partitions));const [replication,setReplication]=useState(String(source.replicationFactor));
  // undefined until the source configuration loads; then holds the settings the copy will carry.
  const [settings,setSettings]=useState<Record<string,string>>();const [adding,setAdding]=useState('');
  const [busy,setBusy]=useState(false);const [error,setError]=useState('');
  const entries=(config.data?.data??[]).filter(e=>!e.sensitive);
  const current=settings??Object.fromEntries(entries.filter(e=>e.override&&e.value!=null).map(e=>[e.name,e.value as string]));
  const available=entries.filter(e=>!(e.name in current));
  const update=(next:Record<string,string>)=>setSettings(next);
  async function submit(e:React.FormEvent){e.preventDefault();setBusy(true);setError('');try{
    await api(topicPath(clusterId,source.name)+'/copy',{method:'POST',body:JSON.stringify({name,partitions:Number(partitions),replicationFactor:Number(replication),config:current})});
    await queryClient.invalidateQueries();onCreated(name);
  }catch(err){setError((err as Error).message)}finally{setBusy(false)}}
  return <Modal open onClose={onClose} wide title="Copy topic" description="Create a new topic from this topic's layout and configuration overrides. Records are not copied."><form onSubmit={submit}>
    <div className="review-destination">Source<strong>{source.name}</strong></div>
    <label>New topic name<input autoFocus value={name} onChange={e=>setName(e.target.value)} pattern="[a-zA-Z0-9._\-]+" maxLength={249} required/></label>
    <div className="copy-grid"><label>Partitions<input type="number" min="1" max="10000" value={partitions} onChange={e=>setPartitions(e.target.value)} required/></label><label>Replication factor<input type="number" min="1" max="100" value={replication} onChange={e=>setReplication(e.target.value)} required/></label></div>
    <fieldset className="copy-settings"><legend>Configuration overrides</legend>
      {config.isLoading?<p className="muted">Loading source configuration…</p>:config.error?<div className="inline-error" role="alert">{config.error.message}</div>:<>
        {Object.keys(current).length===0&&<p className="muted">The source uses cluster defaults only. Add settings to override them on the copy.</p>}
        {Object.entries(current).sort(([a],[b])=>a.localeCompare(b)).map(([key,value])=><div className="copy-setting" key={key}><label>{key}<input value={value} onChange={e=>update({...current,[key]:e.target.value})} aria-label={key}/></label><button type="button" className="icon-button" aria-label={`Remove ${key}`} onClick={()=>{const next={...current};delete next[key];update(next)}}><X size={14}/></button></div>)}
        {available.length>0&&<div className="copy-setting"><select value={adding} onChange={e=>setAdding(e.target.value)} aria-label="Setting to add"><option value="">Add a setting…</option>{available.map(e=><option key={e.name} value={e.name}>{e.name}</option>)}</select><button type="button" className="button" disabled={!adding} onClick={()=>{update({...current,[adding]:entries.find(e=>e.name===adding)?.value??''});setAdding('')}}><Plus size={14}/>Add</button></div>}
      </>}
    </fieldset>
    {error&&<div className="inline-error" role="alert">{error}</div>}
    <button className="button primary" disabled={busy||!config.data||!name||name===source.name}>{busy?'Creating…':'Create copy'}<ArrowRight size={14}/></button>
  </form></Modal>
}

type Outcome={topic:string;error?:string};
/** Deletes the selected topics one by one through the audited single-topic endpoint. */
export function DeleteTopics({clusterId,topics,internal,session,onDone}:{clusterId:string;topics:string[];internal:string[];session?:Session;onDone:(deleted:string[])=>void}){
  const [open,setOpen]=useState(false);const [confirmation,setConfirmation]=useState('');const [busy,setBusy]=useState(false);const [results,setResults]=useState<Outcome[]>([]);
  const queryClient=useQueryClient();
  const phrase=`delete ${topics.length} topic${topics.length===1?'':'s'}`;
  const failed=results.filter(r=>r.error);
  function show(){setOpen(true);setConfirmation('');setResults([])}
  async function submit(e:React.FormEvent){e.preventDefault();setBusy(true);const out:Outcome[]=[];
    for(const topic of topics){try{await api(topicPath(clusterId,topic),{method:'DELETE',body:JSON.stringify({confirmation:topic})});out.push({topic})}catch(err){out.push({topic,error:(err as Error).message})}setResults([...out])}
    setBusy(false);await queryClient.invalidateQueries();
    const deleted=out.filter(r=>!r.error).map(r=>r.topic);onDone(deleted);if(deleted.length===topics.length)setOpen(false);
  }
  return <><button className="button danger-button" disabled={!can(session,clusterId,'delete')} onClick={show}><Trash2 size={14}/>Delete selected</button>
    <Modal open={open} onClose={()=>{if(!busy)setOpen(false)}} title={`Delete ${topics.length} topic${topics.length===1?'':'s'}`} description="Each topic is deleted separately with its own authorization check and audit entry."><form onSubmit={submit}>
      <ul className="bulk-topic-list">{topics.map(t=>{const r=results.find(x=>x.topic===t);return <li key={t}><span className="mono">{t}</span>{internal.includes(t)&&<span className="tag tag-internal">INTERNAL</span>}{r&&(r.error?<span className="bad-text" title={r.error}>Failed</span>:<span className="good-text">Deleted</span>)}</li>})}</ul>
      <div className="mutation-warning"><Trash2 size={16}/><p>Deletion removes these topics and their records. This cannot be rolled back through Kaflux.</p></div>
      {internal.length>0&&<div className="mutation-warning"><Shield size={16}/><p>The selection includes {internal.length} internal topic{internal.length===1?'':'s'}. Deleting them can break consumer groups, transactions or connected services, and Kafka may refuse.</p></div>}
      {failed.length>0&&<div className="inline-error" role="alert">{failed.length} deletion{failed.length===1?'':'s'} failed: {failed.map(f=>`${f.topic}: ${f.error}`).join('; ')}</div>}
      <label>Type <strong className="mono">{phrase}</strong> to confirm<input autoFocus value={confirmation} onChange={e=>setConfirmation(e.target.value)} required autoComplete="off"/></label>
      <button className="button danger-button" disabled={busy||confirmation!==phrase||results.length>0}>{busy?`Deleting ${results.length+1} of ${topics.length}…`:'Confirm deletion'}<ArrowRight size={14}/></button>
    </form></Modal></>
}
