import {useQuery} from '@tanstack/react-query';
import {api} from './api';
import {useSteadyInterval} from './RefreshControl';
import {SortTh,useSortedRows} from './table-sort';

type Offset={topic:string;partition:number;committedOffset:number;startOffset:number;endOffset:number;lag:number};
type Consumer={id:string;state:string;members:number;lag:number|null;offsets:Offset[]};
type Row=Offset&{group:string;state:string};
export function useTopicConsumers(clusterId:string,topic:string){
  const path=`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}/consumers`;
  const consumersEvery=useSteadyInterval(10_000);
  return useQuery({queryKey:[path],queryFn:()=>api<Consumer[]>(path),refetchInterval:consumersEvery});
}
export const consumerGroupHref=(clusterId:string,group:string)=>`/clusters/${clusterId}/consumer-groups?group=${encodeURIComponent(group)}`;
export function TopicConsumers({clusterId,topic}:{clusterId:string;topic:string}){
  const query=useTopicConsumers(clusterId,topic);
  const consumers=query.data?.data??[];
  const rows=useSortedRows(consumers.flatMap(g=>g.offsets.map<Row>(o=>({...o,group:g.id,state:g.state}))),{group:r=>r.group,state:r=>r.state,partition:r=>r.partition,committed:r=>r.committedOffset,end:r=>r.endOffset,lag:r=>r.lag});
  return <section className="panel"><div className="panel-title"><div><h2>{topic} · Consumers</h2><p>Committed offsets and lag per partition for every group consuming this topic.</p></div></div>{query.error&&<div className="inline-error" role="alert">{query.error.message}</div>}{query.isLoading?<p className="empty" role="status">Retrieving consumer offsets…</p>:!consumers.length?<p className="empty">No consumer group has committed offsets on this topic.</p>:<><div className="detail-grid">{consumers.map(g=><div key={g.id}><span><a className="link" href={consumerGroupHref(clusterId,g.id)}>{g.id}</a> · {g.state}</span><strong>{g.lag?.toLocaleString()??'Unavailable'}</strong></div>)}</div><div className="table-scroll"><table><thead><tr><SortTh label="Group" sortKey="group" table={rows}/><SortTh label="State" sortKey="state" table={rows}/><SortTh label="Partition" sortKey="partition" table={rows}/><SortTh label="Committed offset" sortKey="committed" table={rows}/><SortTh label="End offset" sortKey="end" table={rows}/><SortTh label="Lag" sortKey="lag" table={rows}/></tr></thead><tbody>{rows.sorted.map(r=><tr key={`${r.group}-${r.partition}`}><td data-label="Group"><a className="link" href={consumerGroupHref(clusterId,r.group)}>{r.group}</a></td><td data-label="State">{r.state}</td><td data-label="Partition">{r.partition}</td><td data-label="Committed offset">{r.committedOffset<0?'Not committed':r.committedOffset}</td><td data-label="End offset">{r.endOffset}</td><td data-label="Lag">{r.lag.toLocaleString()}</td></tr>)}</tbody></table></div></>}</section>
}
