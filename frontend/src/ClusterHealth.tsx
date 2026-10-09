import {useEffect,useState} from 'react';
import {ChevronDown} from 'lucide-react';
import {api,type MetricResult} from './api';

type Overview={urp:number;offline:number;controller:number|null};
type OverviewResponse={totals:Overview};

/** Sum of the newest sample of every series, or undefined when the metric is unavailable. */
export function latestSum(result:MetricResult|undefined){
  if(result?.status!=='available'||!result.series.length)return undefined;
  let total=0;
  for(const s of result.series){
    const point=(s.points??s.values)?.at(-1);
    const v=point==null?NaN:Number(Array.isArray(point)?point[1]:point.value);
    if(!Number.isFinite(v))return undefined;
    total+=v;
  }
  return total;
}

/** Collapsible cluster health line shown with a rebalance: URP, offline partitions, ISR shrinks and the controller. */
export function ClusterHealth({clusterId,live}:{clusterId:string;live:boolean}){
  const [overview,setOverview]=useState<Overview|null>(null);
  const [shrinks,setShrinks]=useState<number|undefined|null>(null);
  useEffect(()=>{
    let stop=false;
    const base=`/clusters/${encodeURIComponent(clusterId)}`;
    async function load(){
      try{const r=await api<OverviewResponse>(`${base}/overview`);if(!stop)setOverview(r.data.totals)}catch{/* keep the last value */}
      try{
        const catalog=await api<{id:string;title:string}[]>(`${base}/metrics/catalog`);
        const metric=catalog.data.find(m=>/isr shrinks/i.test(m.title));
        if(!metric){if(!stop)setShrinks(undefined);return}
        const r=await api<MetricResult>(`${base}/metrics/query?metric=${encodeURIComponent(metric.id)}&range=5m`);
        if(!stop)setShrinks(latestSum(r.data));
      }catch{if(!stop)setShrinks(undefined)}
    }
    load();
    if(!live)return ()=>{stop=true};
    const id=setInterval(load,15_000);
    return ()=>{stop=true;clearInterval(id)};
  },[clusterId,live]);
  const healthy=!!overview&&overview.urp===0&&overview.offline===0&&overview.controller!=null&&overview.controller>=0;
  const lead=!overview?'Checking cluster health…':healthy?(live?'✓ Cluster preflight passed':'✓ Cluster healthy'):'⚠ Cluster needs attention';
  return <details className="cluster-health">
    <summary>
      <span className={`ch-lead ${!overview?'':healthy?'good':'warn'}`}>{lead}</span>
      <span className="ch-mid">{overview?`${overview.urp} URP · ${overview.offline} offline · controller ${overview.controller!=null&&overview.controller>=0?'stable':'unknown'}`:''}</span>
      <span className="be-toggle">Health details <ChevronDown size={13}/></span>
    </summary>
    <div className="ch-body">
      <div><span>Under-replicated</span><strong className={overview&&overview.urp>0?'warn':''}>{overview?.urp??'—'}</strong><small>partitions, cluster-wide</small></div>
      <div><span>Offline partitions</span><strong className={overview&&overview.offline>0?'bad':''}>{overview?.offline??'—'}</strong><small>no leader</small></div>
      <div><span>ISR shrinks</span><strong className={shrinks?'warn':''}>{shrinks==null?'—':`${shrinks.toFixed(shrinks<10?1:0)}/s`}</strong><small>{shrinks===undefined?'metrics not configured':'last 5 min'}</small></div>
      <div><span>Controller</span><strong className={overview&&(overview.controller==null||overview.controller<0)?'bad':''}>{overview?(overview.controller!=null&&overview.controller>=0?'Stable':'Unknown'):'—'}</strong><small>{overview?.controller!=null&&overview.controller>=0?`broker ${overview.controller}`:' '}</small></div>
    </div>
  </details>;
}
