import {useQuery} from '@tanstack/react-query';
import {AlertTriangle,ArrowRight,CircleAlert,Info,Layers3,RefreshCw,Shield} from 'lucide-react';
import type {CSSProperties} from 'react';
import {api,bytes} from './api';
import {SortButton,useSortedRows} from './table-sort';
import {useTimeZone} from './TimeZone';

type Deviation={broker:number;value:number;differenceFromMean:number;percentageDifference:number};
export type Dimension={id:string;status:'GOOD'|'MODERATE'|'HIGH_SKEW'|'UNAVAILABLE';mean:number;maxMinusMin:number;coefficientOfVariation:number;maxToMeanRatio:number;brokers:Deviation[];reason?:string};
type TopicBroker={broker:number;rack:string;replicas:number;leaders:number;preferredReplicas:number;outOfSync:number;bytes:number|null};
type TopicPartition={id:number;leader:number;replicas:number[];isr:number[];sizeBytes:number|null;preferredLeader:boolean;underReplicated:boolean;offline:boolean;singleRack:boolean};
type Finding={severity:'critical'|'warning'|'info';kind:string;message:string;partition?:number;broker?:number};
export type TopicAnalysis={topic:string;replicationFactor:number;brokers:TopicBroker[];partitions:TopicPartition[];dimensions:Dimension[];preferredLeaderRatio:number;urp:number;offline:number;rackAware:boolean;singleRackPartitions:number;findings:Finding[];observedAt:string};

const statusLabel={GOOD:'Balanced',MODERATE:'Moderate skew',HIGH_SKEW:'High skew',UNAVAILABLE:'Unavailable'};
const severityIcon={critical:CircleAlert,warning:AlertTriangle,info:Info};
// Broker identity follows broker position in the cluster, never rank, so a broker keeps its color across views.
export const brokerColor=(i:number)=>i<8?`var(--series-${i+1})`:'var(--muted)';
const percent=(x:number)=>`${(x*100).toFixed(1)}%`;

export function SkewBadge({status}:{status:Dimension['status']}){return <span className={`skew-badge ${status.toLowerCase()}`}><i aria-hidden="true"/>{statusLabel[status]}</span>}

function role(p:TopicPartition,broker:number){if(!p.replicas.includes(broker))return null;if(p.leader===broker)return {kind:'leader',mark:'L',text:'Leader'};if(!p.isr.includes(broker))return {kind:'lagging',mark:'!',text:'Out of sync'};return {kind:'follower',mark:'F',text:'In-sync follower'}}

export function TopicDistribution({clusterId,topic,onPlan}:{clusterId:string;topic:string;onPlan?:()=>void}){
  const tz=useTimeZone();const query=useQuery({queryKey:[`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/balance`],queryFn:()=>api<TopicAnalysis>(`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/balance`)});
  const a=query.data?.data;
  const dimension=(id:string)=>a?.dimensions.find(d=>d.id===id);
  const replicas=dimension('replicas'),leaders=dimension('leaders'),storage=dimension('bytes');
  const deviation=(d:Dimension|undefined,broker:number)=>d?.brokers.find(x=>x.broker===broker)?.percentageDifference;
  const maxReplicas=Math.max(...(a?.brokers??[]).map(b=>b.replicas),1);
  const colored=(a?.brokers??[]).map((b,i)=>({...b,color:brokerColor(i)}));
  const brokers=useSortedRows(colored,{broker:b=>b.broker,replicas:b=>b.replicas,leaders:b=>b.leaders,bytes:b=>b.bytes});
  const columns=[...colored].sort((x,y)=>x.rack.localeCompare(y.rack)||x.broker-y.broker);
  const analyze=<button className="button primary" onClick={()=>query.refetch()} disabled={query.isFetching}><RefreshCw size={13} className={query.isFetching?'spin':undefined}/>{query.isFetching?'Analyzing…':'Analyze'}</button>;
  return <section className="panel topic-distribution">
    <div className="panel-title"><div><h2>Topic distribution</h2><p>{a?`How ${topic} spreads across ${a.brokers.length} brokers · observed ${tz.time(a.observedAt)} · source: Kafka Admin API`:'Partition, replica and leader placement per broker'}</p></div><div className="heading-actions">{onPlan&&<button className="button" onClick={onPlan}>Plan reassignment <ArrowRight size={13}/></button>}{analyze}</div></div>
    {query.error&&<div className="error-box" role="alert"><Shield size={17}/><div><strong>Unable to analyze this topic</strong><p>{query.error.message}</p></div></div>}
    {query.isLoading?<div className="loading" role="status"><span aria-hidden="true"/>Analyzing placement…</div>:a&&<>
      <div className="distribution-kpis">
        {[replicas,leaders].map(d=>d&&<div key={d.id}><span>{d.id==='replicas'?'Replica skew':'Leader skew'}</span><strong>{percent(d.coefficientOfVariation)}</strong><SkewBadge status={d.status}/><small>{d.reason??`max ${d.maxToMeanRatio.toFixed(2)}× mean · spread ${d.maxMinusMin}`}</small></div>)}
        <div><span>Preferred leaders</span><strong>{percent(a.preferredLeaderRatio)}</strong><span className={`skew-badge ${a.preferredLeaderRatio===1?'good':'moderate'}`}><i aria-hidden="true"/>{a.partitions.filter(p=>p.preferredLeader).length} of {a.partitions.length}</span><small>Leader is the first assigned replica</small></div>
        <div><span>Replica health</span><strong>{a.offline+a.urp}</strong><span className={`skew-badge ${a.offline?'high_skew':a.urp?'moderate':'good'}`}><i aria-hidden="true"/>{a.offline?`${a.offline} offline`:a.urp?`${a.urp} under-replicated`:'All in sync'}</span><small>{a.rackAware?`${a.singleRackPartitions} single-rack partitions`:'Rack awareness not configured'} · RF {a.replicationFactor}</small></div>
      </div>
      <div className="distribution-grid">
        <div className="distribution-brokers" aria-label="Per-broker placement">
          <div className="distribution-sort"><h3>Per broker</h3><div role="group" aria-label="Sort brokers">{([['broker','Broker'],['replicas','Replicas'],['leaders','Leaders'],['bytes','Size']] as const).map(([key,label])=><SortButton key={key} direction={brokers.direction(key)} pressed={!!brokers.direction(key)} onClick={()=>brokers.toggle(key)}>{label}</SortButton>)}</div></div>
          {brokers.sorted.map(b=><div className="distribution-broker" key={b.broker}>
            <div><span><i style={{background:b.color}} aria-hidden="true"/>broker-{b.broker}</span>{b.rack&&<small className="tag">{b.rack}</small>}</div>
            <div className="bar-track" title={`${b.replicas} replicas, ${b.leaders} leaders`}><div style={{width:`${b.replicas/maxReplicas*100}%`,background:`color-mix(in srgb,${b.color} 38%,transparent)`}}/><div style={{width:`${b.leaders/maxReplicas*100}%`,background:b.color}}/></div>
            <dl><div><dt>Replicas</dt><dd>{b.replicas}<Delta value={deviation(replicas,b.broker)}/></dd></div><div><dt>Leaders</dt><dd>{b.leaders}<Delta value={deviation(leaders,b.broker)}/></dd></div><div><dt>Size</dt><dd>{b.bytes==null?'—':bytes(b.bytes)}<Delta value={deviation(storage,b.broker)}/></dd></div>{b.outOfSync>0&&<div><dt>Out of sync</dt><dd className="warn-text">{b.outOfSync}</dd></div>}</dl>
          </div>)}
          <p className="distribution-legend"><span><i className="solid"/>Leaders</span><span><i className="tint"/>Replicas</span>{storage?.status==='UNAVAILABLE'&&<span>{storage.reason}</span>}</p>
        </div>
        <div className="distribution-findings"><h3>Findings</h3>{a.findings.length?a.findings.map((f,i)=>{const Icon=severityIcon[f.severity];return <div key={i} className={`finding ${f.severity}`}><Icon size={14} aria-hidden="true"/><div><strong>{f.severity==='critical'?'Critical':f.severity==='warning'?'Warning':'Note'}</strong><p>{f.message}</p></div></div>}):<div className="finding good"><Shield size={14} aria-hidden="true"/><div><strong>Healthy placement</strong><p>Replicas and leaders are evenly spread and every replica is in sync.</p></div></div>}</div>
      </div>
      <div className="placement-matrix">
        <h3>Placement matrix <small>rows are partitions, columns are brokers grouped by rack</small></h3>
        {a.partitions.length?<div className="table-scroll"><table>
          <thead><tr><th>Partition</th>{columns.map(b=><th key={b.broker} style={{boxShadow:`inset 0 -2px ${b.color}`}}>broker-{b.broker}{b.rack&&<small>{b.rack}</small>}</th>)}<th>State</th></tr></thead>
          <tbody>{a.partitions.map(p=><tr key={p.id}><td>p{p.id}</td>{columns.map(b=>{const r=role(p,b.broker);const preferred=p.replicas[0]===b.broker;return <td key={b.broker}>{r&&<span className={`placement-cell ${r.kind}`} style={{'--cell':b.color} as CSSProperties} title={`Partition ${p.id} on broker ${b.broker}: ${r.text}${preferred?' · preferred leader':''}`} aria-label={`${r.text}${preferred?', preferred leader':''}`}>{r.mark}{preferred&&<sup aria-hidden="true">•</sup>}</span>}</td>})}<td><PartitionState p={p}/></td></tr>)}</tbody>
        </table></div>:<div className="empty"><Layers3 size={28}/><p>This topic has no partitions.</p></div>}
        <p className="distribution-legend"><span><i className="cell leader">L</i>Leader</span><span><i className="cell follower">F</i>In-sync follower</span><span><i className="cell lagging">!</i>Out of sync</span><span>• Preferred leader</span></p>
      </div>
    </>}
  </section>
}

export function Delta({value}:{value?:number}){if(value==null||Math.abs(value)<.5)return null;return <small className={Math.abs(value)>=25?'bad-text':Math.abs(value)>=10?'warn-text':''}>{value>0?'+':''}{value.toFixed(0)}%</small>}

function PartitionState({p}:{p:TopicPartition}){const states=[p.offline&&['high_skew','Offline'],p.underReplicated&&['moderate',`ISR ${p.isr.length}/${p.replicas.length}`],p.singleRack&&['moderate','Single rack'],!p.offline&&!p.preferredLeader&&['neutral','Non-preferred leader']].filter(Boolean) as string[][];return states.length?<>{states.map(([kind,label])=><span key={label} className={`skew-badge ${kind}`}><i aria-hidden="true"/>{label}</span>)}</>:<span className="skew-badge good"><i aria-hidden="true"/>Healthy</span>}
