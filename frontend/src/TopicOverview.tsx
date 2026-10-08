import {ArrowRight} from 'lucide-react';
import {bytes} from './api';
import {SortTh,useSortedRows} from './table-sort';
import {useTopicConfig} from './TopicConfiguration';
import {consumerGroupHref,useTopicConsumers} from './TopicConsumers';
import {describeConfig,humanConfigValue} from './topic-config';

export type TopicPartition={id:number;leader:number;replicas:number[];isr:number[];startOffset:number|null;endOffset:number|null;sizeBytes:number|null};
export type TopicDetail={name:string;partitions:TopicPartition[];replicationFactor:number;sizeBytes:number|null;urp:number;cleanupPolicy:string;retentionMs:number|null;observedAt:string};
const highlights=['cleanup.policy','retention.ms','retention.bytes','min.insync.replicas','max.message.bytes','compression.type'];
const records=(p:TopicPartition)=>p.startOffset==null||p.endOffset==null?null:p.endOffset-p.startOffset;

export function TopicOverview({clusterId,topic,onTab}:{clusterId:string;topic:TopicDetail;onTab:(tab:string)=>void}){
  const config=useTopicConfig(clusterId,topic.name);const consumers=useTopicConsumers(clusterId,topic.name);const entries=new Map((config.data?.data??[]).map(e=>[e.name,e]));
  const partitions=topic.partitions;const counts=partitions.map(records);const known=counts.every(x=>x!=null);const total=counts.reduce<number>((a,b)=>a+(b??0),0);
  const leaders=new Set(partitions.map(p=>p.leader)).size;const minIsr=entries.get('min.insync.replicas')?.value;
  const stats:[string,string,string][]=[
    ['Partitions',String(partitions.length),`Led by ${leaders} ${leaders===1?'broker':'brokers'}`],
    ['Replication factor',String(topic.replicationFactor),`${minIsr?`Min ISR ${minIsr}`:'Copies of each partition'}`],
    ['Under-replicated',String(topic.urp),topic.urp?'Partitions with replicas missing from ISR':'No partitions behind'],
    ['Retained records',known?total.toLocaleString():'Unavailable','Sum of end minus start offsets'],
    ['Size on disk',bytes(topic.sizeBytes),topic.sizeBytes==null?'Broker log size not reported':`Across ${partitions.length} partitions`],
  ];
  return <>
    <div className="stat-grid topic-stats">{stats.map(([label,value,note],i)=><div className="stat-card" key={label} style={{'--i':i} as React.CSSProperties}><div>{label}</div><strong>{value}</strong><small><i className={label==='Under-replicated'&&topic.urp?'warn':undefined}/>{note}</small></div>)}</div>
    <section className="panel"><div className="panel-title"><div><h2>Key settings</h2><p>The configuration that most shapes how this topic stores and accepts data.</p></div><button className="button" onClick={()=>onTab('Configuration')}>All configuration <ArrowRight size={14}/></button></div>{config.error&&<div className="inline-error" role="alert">{config.error.message}</div>}<div className="detail-grid key-settings">{highlights.map(name=>{const e=entries.get(name);const raw=e?.value??(name==='cleanup.policy'?topic.cleanupPolicy:name==='retention.ms'&&topic.retentionMs!=null?String(topic.retentionMs):null);const human=raw==null?undefined:humanConfigValue(name,raw);return <div key={name} title={describeConfig(name)}><span className="mono">{name}{e?.override&&<em>override</em>}</span><strong>{raw==null?config.isLoading?'Loading…':'Unavailable':human??raw}</strong>{human&&<small className="mono">{raw}</small>}</div>})}</div><div className="panel-bottom"><span>Observed {new Date(topic.observedAt).toLocaleString()}</span><button className="text-button" onClick={()=>onTab('Partitions')}>View {partitions.length} partitions →</button></div></section>
    <section className="panel"><div className="panel-title"><div><h2>Consumers</h2><p>Groups consuming from this topic, with their lag on it.</p></div><button className="button" onClick={()=>onTab('Consumers')}>Lag by partition <ArrowRight size={14}/></button></div>{consumers.error&&<div className="inline-error" role="alert">{consumers.error.message}</div>}{consumers.isLoading?<p className="empty" role="status">Retrieving consumer groups…</p>:!consumers.data?.data.length?<p className="empty">No consumer group has committed offsets on this topic.</p>:<div className="table-scroll"><table><thead><tr><th scope="col">Group</th><th scope="col">State</th><th scope="col">Members</th><th scope="col">Lag</th></tr></thead><tbody>{consumers.data.data.map(g=><tr key={g.id}><td data-label="Group"><a className="link" href={consumerGroupHref(clusterId,g.id)}>{g.id}</a></td><td data-label="State">{g.state}</td><td data-label="Members">{g.members}</td><td data-label="Lag">{g.lag?.toLocaleString()??'Unavailable'}</td></tr>)}</tbody></table></div>}</section>
  </>
}

export function PartitionTable({partitions}:{partitions:TopicPartition[]}){
  const rows=useSortedRows(partitions,{id:p=>p.id,leader:p=>p.leader,isr:p=>p.isr.length,start:p=>p.startOffset,end:p=>p.endOffset,records:records,size:p=>p.sizeBytes});
  if(!partitions.length)return <p className="empty">This topic has no partitions.</p>;
  return <div className="table-scroll"><table><thead><tr><SortTh label="Partition" sortKey="id" table={rows}/><SortTh label="Leader" sortKey="leader" table={rows}/><th scope="col">Replicas</th><SortTh label="In-sync" sortKey="isr" table={rows}/><SortTh label="Start offset" sortKey="start" table={rows}/><SortTh label="End offset" sortKey="end" table={rows}/><SortTh label="Records" sortKey="records" table={rows}/><SortTh label="Size" sortKey="size" table={rows}/></tr></thead><tbody>{rows.sorted.map(p=>{const lagging=p.replicas.filter(r=>!p.isr.includes(r));return <tr key={p.id}><td data-label="Partition">{p.id}</td><td data-label="Leader">broker-{p.leader}</td><td data-label="Replicas"><span className="replica-list">{p.replicas.map(r=><span key={r} className={`tag${r===p.leader?' replica-leader':''}${lagging.includes(r)?' replica-lagging':''}`} title={r===p.leader?'Leader':lagging.includes(r)?'Out of sync':'In sync'}>{r}</span>)}</span></td><td data-label="In-sync">{lagging.length?<span className="replica-lagging">{p.isr.length} of {p.replicas.length}</span>:`${p.isr.length} of ${p.replicas.length}`}</td><td data-label="Start offset">{p.startOffset?.toLocaleString()??'—'}</td><td data-label="End offset">{p.endOffset?.toLocaleString()??'—'}</td><td data-label="Records">{records(p)?.toLocaleString()??'—'}</td><td data-label="Size">{bytes(p.sizeBytes)}</td></tr>})}</tbody></table></div>
}
