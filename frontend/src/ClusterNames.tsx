import {useState} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
import {Pencil,RotateCcw} from 'lucide-react';
import {api,can,type Cluster,type Session} from './api';
import './ClusterNames.css';

function ClusterNameRow({cluster,current,admin}:{cluster:Cluster;current:boolean;admin:boolean}){
  const [editing,setEditing]=useState(false);const [name,setName]=useState(cluster.name);const [error,setError]=useState('');const [notice,setNotice]=useState('');const [busy,setBusy]=useState(false);
  const queryClient=useQueryClient();const renamed=cluster.name!==cluster.configuredName;
  function edit(){setName(cluster.name);setError('');setNotice('');setEditing(true)}
  async function save(value:string){setBusy(true);setError('');setNotice('');try{const result=await api<{name:string}>(`/clusters/${encodeURIComponent(cluster.id)}/name`,{method:'PUT',body:JSON.stringify({name:value})});await queryClient.invalidateQueries({queryKey:['/clusters']});setEditing(false);setNotice(`Renamed to ${result.data.name}.`)}catch(e){setError((e as Error).message)}finally{setBusy(false)}}
  const trimmed=name.trim();
  return <li className={current?'current':undefined}>{editing?<form onSubmit={e=>{e.preventDefault();save(trimmed)}} onKeyDown={e=>{if(e.key==='Escape'){e.preventDefault();setEditing(false);setError('')}}}><input autoFocus aria-label={`New name for ${cluster.name}`} value={name} onChange={e=>setName(e.target.value)} maxLength={64} required placeholder={cluster.configuredName}/><button type="button" className="button" disabled={busy} onClick={()=>{setEditing(false);setError('')}}>Cancel</button><button className="button primary" disabled={busy||!trimmed||trimmed===cluster.name}>{busy?'Saving…':'Save'}</button></form>:<><div className="cluster-name-label"><strong>{cluster.name}{current&&<span className="tiny-badge">CURRENT</span>}</strong><small>{cluster.id}{renamed&&` · configured as ${cluster.configuredName}`}</small></div><div className="cluster-name-actions">{renamed&&<button type="button" className="button" aria-label={`Reset ${cluster.name} to ${cluster.configuredName}`} disabled={!admin||busy} onClick={()=>save('')}><RotateCcw size={14}/>Reset</button>}<button type="button" className="button" aria-label={`Rename ${cluster.name}`} disabled={!admin||busy} onClick={edit}><Pencil size={14}/>Rename</button></div></>}{error&&<div className="inline-error" role="alert">{error}</div>}{notice&&<div className="inline-notice" role="status">{notice}</div>}</li>
}

/** Display names for every cluster the user can read; names are unique and shared by all users. */
export function ClusterNames({clusterId,session}:{clusterId:string;session?:Session}){
  const clusters=useQuery({queryKey:['/clusters'],queryFn:()=>api<Cluster[]>('/clusters')});
  const list=[...clusters.data?.data??[]].sort((a,b)=>Number(b.id===clusterId)-Number(a.id===clusterId));
  return <section className="panel"><div className="panel-title"><div><h2>Cluster names</h2><p>How each cluster appears in Kaflux for every user. Names must be unique; reset restores the configured name.</p></div><Pencil size={18}/></div>{clusters.error&&<div className="inline-error" role="alert">{clusters.error.message}</div>}<ul className="cluster-names">{list.map(c=><ClusterNameRow key={c.id} cluster={c} current={c.id===clusterId} admin={can(session,c.id,'rename')}/>)}</ul></section>
}
