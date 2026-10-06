import {useState} from 'react';
import {useQuery} from '@tanstack/react-query';
import {ArrowRight,RotateCcw,Search} from 'lucide-react';
import {api} from './api';
import {configGroup,configGroups,configOptions,describeConfig,humanConfigValue,updateMode,type ConfigEntry} from './topic-config';

const sources:Record<string,string>={DYNAMIC_TOPIC_CONFIG:'Topic override',DYNAMIC_BROKER_CONFIG:'Broker',DYNAMIC_DEFAULT_BROKER_CONFIG:'Cluster default',STATIC_BROKER_CONFIG:'Broker file',DEFAULT_CONFIG:'Kafka default'};
export function useTopicConfig(clusterId:string,topic:string){
  const path=`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/config`;
  return useQuery({queryKey:[path],queryFn:()=>api<ConfigEntry[]>(path),refetchOnMount:'always'});
}
function grouped(entries:ConfigEntry[]){return configGroups.map(group=>[group,entries.filter(e=>configGroup(e.name)===group)] as const).filter(([,rows])=>rows.length)}
function matches(e:ConfigEntry,q:string){q=q.trim().toLowerCase();return !q||e.name.includes(q)||(e.value??'').toLowerCase().includes(q)||(describeConfig(e.name)??'').toLowerCase().includes(q)}
function ConfigFilters({q,setQ,overridesOnly,setOverridesOnly,overrides}:{q:string;setQ:(v:string)=>void;overridesOnly:boolean;setOverridesOnly:(v:boolean)=>void;overrides:number}){
  return <div className="table-toolbar"><div className="search-input"><Search size={15}/><input aria-label="Filter configuration" placeholder="Filter keys, values or descriptions…" value={q} onChange={e=>setQ(e.target.value)}/></div><div className="segmented" role="group" aria-label="Configuration scope"><button type="button" className={overridesOnly?'':'selected'} aria-pressed={!overridesOnly} onClick={()=>setOverridesOnly(false)}>All</button><button type="button" className={overridesOnly?'selected':''} aria-pressed={overridesOnly} onClick={()=>setOverridesOnly(true)}>Overrides · {overrides}</button></div></div>
}
function ConfigValue({entry}:{entry:ConfigEntry}){
  const human=humanConfigValue(entry.name,entry.value);
  return <div className="config-value"><strong>{human??entry.value}</strong>{human&&entry.value!=null&&entry.value!==''&&<span className="mono">{entry.value}</span>}</div>
}
function UpdateModeTag({name}:{name:string}){const mode=updateMode(name);return mode?<span className={`config-mode ${mode.kind}`} title={mode.detail}>{mode.label}</span>:null}
function ConfigName({entry}:{entry:ConfigEntry}){return <div className="config-name"><span className="mono">{entry.name}</span>{describeConfig(entry.name)&&<small>{describeConfig(entry.name)}</small>}<UpdateModeTag name={entry.name}/></div>}
function UpdateModeLegend(){return <div className="config-legend"><span><span className="config-mode dynamic">Dynamic default</span>Broker-wide default changeable live (cluster-wide)</span><span><span className="config-mode static">Static default</span>Broker default read-only: server.properties + restart</span><span><span className="config-mode topic">Topic only</span>No broker default; per-topic setting</span><small>Per the Kafka 3.7–4.1 reference, every topic override applies live. Hover a tag for the broker property.</small></div>}

export function TopicConfiguration({clusterId,topic}:{clusterId:string;topic:string}){
  const query=useTopicConfig(clusterId,topic);const [q,setQ]=useState('');const [overridesOnly,setOverridesOnly]=useState(false);
  const entries=query.data?.data??[];const overrides=entries.filter(e=>e.override).length;
  const groups=grouped(entries.filter(e=>(!overridesOnly||e.override)&&matches(e,q)));
  return <section className="panel"><div className="panel-title"><div><h2>{topic} · Configuration</h2><p>{entries.length} topic-level keys · {overrides} overridden on this topic, the rest inherited from broker or Kafka defaults. </p></div></div><UpdateModeLegend/><ConfigFilters q={q} setQ={setQ} overridesOnly={overridesOnly} setOverridesOnly={setOverridesOnly} overrides={overrides}/>{query.error&&<div className="inline-error" role="alert">{query.error.message}</div>}{query.isLoading?<p className="empty" role="status">Retrieving topic configuration…</p>:!groups.length?<p className="empty">No configuration keys match this filter.</p>:groups.map(([group,rows])=><div className="config-group" key={group}><h3>{group}<span>{rows.length}</span></h3>{rows.map(e=><div className={`config-row${e.override?' override':''}`} key={e.name}><ConfigName entry={e}/><ConfigValue entry={e}/><span className={e.override?'tag config-override':'tag'}>{sources[e.source]??e.source}</span></div>)}</div>)}</section>
}

export type ConfigChange={config:Record<string,string>;reset:string[]};
export function TopicConfigEditor({entries,busy,error,onSubmit}:{entries:ConfigEntry[];busy:boolean;error:string;onSubmit:(change:ConfigChange)=>void}){
  const [edits,setEdits]=useState<Record<string,string>>({});const [resets,setResets]=useState<string[]>([]);const [q,setQ]=useState('');const [overridesOnly,setOverridesOnly]=useState(false);
  const editable=entries.filter(e=>!e.sensitive);const byName=new Map(editable.map(e=>[e.name,e]));
  const groups=grouped(editable.filter(e=>(!overridesOnly||e.override||e.name in edits)&&matches(e,q)));
  function edit(e:ConfigEntry,value:string){setResets(r=>r.filter(x=>x!==e.name));setEdits(old=>{const next={...old};if(value===(e.value??''))delete next[e.name];else next[e.name]=value;return next})}
  function toggleReset(e:ConfigEntry){setEdits(old=>{const next={...old};delete next[e.name];return next});setResets(r=>r.includes(e.name)?r.filter(x=>x!==e.name):[...r,e.name])}
  const changes=[...Object.entries(edits).map(([name,value])=>({name,from:byName.get(name)?.value??'',to:value})),...resets.map(name=>({name,from:byName.get(name)?.value??'',to:undefined}))].sort((a,b)=>a.name.localeCompare(b.name));
  return <form onSubmit={e=>{e.preventDefault();onSubmit({config:edits,reset:resets})}}>
    <UpdateModeLegend/>
    <ConfigFilters q={q} setQ={setQ} overridesOnly={overridesOnly} setOverridesOnly={setOverridesOnly} overrides={editable.filter(e=>e.override).length}/>
    <div className="config-editor">{!groups.length?<p className="empty">No configuration keys match this filter.</p>:groups.map(([group,rows])=><fieldset className="config-group" key={group}><legend>{group}</legend>{rows.map(e=>{const value=edits[e.name]??e.value??'';const reset=resets.includes(e.name);const options=configOptions(e);const id=`config-${e.name}`;const human=reset?'Reverts to broker default':humanConfigValue(e.name,value);return <div className={`config-row${e.name in edits||reset?' changed':''}${e.override?' override':''}`} key={e.name}><label htmlFor={id} className="config-name"><span className="mono">{e.name}</span>{describeConfig(e.name)&&<small>{describeConfig(e.name)}</small>}<UpdateModeTag name={e.name}/></label><div className="config-input">{options?<select id={id} value={value} disabled={reset} onChange={x=>edit(e,x.target.value)}>{!options.includes(value)&&<option value={value}>{value}</option>}{options.map(o=><option key={o} value={o}>{o}</option>)}</select>:<input id={id} value={value} disabled={reset} spellCheck={false} onChange={x=>edit(e,x.target.value)}/>}{human&&<small>{human}</small>}</div>{e.override?<button type="button" className={`icon-button${reset?' active':''}`} aria-pressed={reset} aria-label={`Reset ${e.name} to default`} title={reset?'Keep topic override':'Remove topic override'} onClick={()=>toggleReset(e)}><RotateCcw size={14}/></button>:<span className="tag">default</span>}</div>})}</fieldset>)}</div>
    {changes.length>0&&<div className="config-review" aria-label="Pending changes"><strong>{changes.length} pending {changes.length===1?'change':'changes'}</strong><ul>{changes.map(c=><li key={c.name}><span className="mono">{c.name}</span><span className="mono">{c.from||'∅'}</span><ArrowRight size={12}/><span className="mono">{c.to===undefined?'default':c.to||'∅'}</span></li>)}</ul></div>}
    {error&&<div className="inline-error" role="alert">{error}</div>}
    <button className="button primary" disabled={busy||!changes.length}>{busy?'Submitting…':changes.length?`Apply ${changes.length} ${changes.length===1?'change':'changes'}`:'No changes'}<ArrowRight size={14}/></button>
  </form>
}
// Loads the authoritative configuration before offering any edit.
export function TopicConfigEdit({clusterId,topic,busy,error,onSubmit}:{clusterId:string;topic:string;busy:boolean;error:string;onSubmit:(change:ConfigChange)=>void}){
  const query=useTopicConfig(clusterId,topic);
  if(query.isPending)return <p className="empty" role="status">Retrieving topic configuration…</p>;
  if(query.error)return <div className="inline-error" role="alert">{query.error.message}</div>;
  return <TopicConfigEditor entries={query.data.data} busy={busy} error={error} onSubmit={onSubmit}/>;
}
