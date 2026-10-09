import type {ThrottlePlan} from './ThrottleControl';

export type StepState='pending'|'moving'|'electing'|'done'|'skipped'|'failed';
export type TopicStep={topic:string;state:StepState;partitions:number;partitionsDone:number;bytes?:number;bytesDone?:number;startedAt?:string;finishedAt?:string;error?:string};
export type JobEvent={seq:number;at:string;level:'info'|'warn'|'error';message:string};
export type Distribution={broker:number;replicas:number;leaders:number;bytes?:number};
export type Change={topic:string;partition:number;before:number[];after:number[];finishedAt?:string};
export type RebalanceJob=ThrottlePlan&{
  changes:Change[];before?:Distribution[]|null;after?:Distribution[]|null;measuredAfter?:Distribution[];createdAt:string;progress?:number;
  startedAt?:string;finishedAt?:string;error?:string;approvedBy?:string;
  steps?:TopicStep[];currentStep?:number;pauseRequested?:boolean;pauseReason?:string;
  rollbackRequested?:boolean;rollbackOf?:string;rollbackJob?:string;warnings?:string[];
  partitionsDone?:number;partitionsTotal?:number;bytesTotal?:number;bytesDone?:number;
  rateBytesPerSec?:number;etaSeconds?:number;etaBasis?:'bytes'|'topics';estimatedBytes?:number|null;
};

/** States that hold the cluster's rebalance lock. */
export const activeJob=(p:{state:string;cleanupPending?:boolean})=>/^(queued|running|paused|rollback-queued)$/.test(p.state)||!!p.cleanupPending;

/** 5m12s, 1h12m, 40s — the console's duration style. */
export function duration(seconds:number){
  const s=Math.max(0,Math.round(seconds));
  if(s>=3600)return `${Math.floor(s/3600)}h${String(Math.floor(s%3600/60)).padStart(2,'0')}m`;
  if(s>=60)return `${Math.floor(s/60)}m${String(s%60).padStart(2,'0')}s`;
  return `${s}s`;
}

/** "12m ago", "yesterday", "3d ago". */
export function ago(iso:string|undefined,now=Date.now()){
  if(!iso)return '';
  const s=(now-new Date(iso).getTime())/1000;
  if(s<60)return 'just now';
  if(s<3600)return `${Math.floor(s/60)}m ago`;
  if(s<86400)return `${Math.floor(s/3600)}h ago`;
  if(s<172800)return 'yesterday';
  return `${Math.floor(s/86400)}d ago`;
}

export function jobSummary(p:RebalanceJob){
  const steps=p.steps??[];
  const finished=steps.filter(s=>s.state==='done'||s.state==='skipped').length;
  const parts:string[]=[];
  if(steps.length&&activeJob(p))parts.push(`${Math.min(finished+(p.state==='running'?1:0),steps.length)}/${steps.length} topics`);
  if(p.state==='running'&&p.etaSeconds!=null)parts.push(`ETA ${duration(p.etaSeconds)}`);
  if(p.state==='paused'&&p.pauseReason)parts.push(p.pauseReason);
  return parts.join(' · ');
}

/** Coefficient of variation in percent (population standard deviation over mean), as the balance advisor reports it. */
export function cv(values:number[]){
  if(!values.length)return undefined;
  const mean=values.reduce((a,b)=>a+b,0)/values.length;
  if(mean===0)return 0;
  const variance=values.reduce((a,b)=>a+(b-mean)**2,0)/values.length;
  return Math.sqrt(variance)/mean*100;
}

/** Per-broker values for one measure; brokers missing on one side count as zero. */
export function brokerValues(before:Distribution[],after:Distribution[],key:'replicas'|'leaders'|'bytes'){
  const ids=[...new Set([...before,...after].map(d=>d.broker))].sort((a,b)=>a-b);
  const pick=(list:Distribution[],id:number)=>list.find(d=>d.broker===id)?.[key]??0;
  const known=key!=='bytes'||[...before,...after].every(d=>d.bytes!=null);
  return {ids,before:ids.map(id=>pick(before,id)),after:ids.map(id=>pick(after,id)),known};
}

/** One-line summary for an Operation history row. */
export function jobNote(p:RebalanceJob){
  const steps=p.steps??[];
  const finished=steps.filter(s=>s.state==='done'||s.state==='skipped').length;
  const total=p.partitionsTotal??p.changes.length;
  const took=p.startedAt&&p.finishedAt?` in ${duration((new Date(p.finishedAt).getTime()-new Date(p.startedAt).getTime())/1000)}`:'';
  switch(p.state){
    case 'planned':return `Planned · ${steps.length} topics · ${total} partitions. Review the changes, then approve to run.`;
    case 'queued':return `Queued · ${steps.length} topics · ${total} partitions. Preflight runs before the first topic.`;
    case 'running':{const current=steps[p.currentStep??0];return `Running · topic ${Math.min(finished+1,steps.length)} of ${steps.length}${current?` (${current.topic})`:''} · ${p.partitionsDone??0} of ${total} partitions${p.etaSeconds!=null?` · ETA ${duration(p.etaSeconds)}`:''}. Original placements are saved for rollback.`}
    case 'paused':return `Paused · ${finished} of ${steps.length} topics moved. ${p.pauseReason??''}`.trim();
    case 'completed':return `Completed${took} · ${finished} of ${steps.length} topics · ${total} partitions${p.warnings?.length?` · ${p.warnings.length} warnings`:''}.`;
    case 'canceled':return `Canceled${took} · ${finished} of ${steps.length} topics moved.${p.rollbackJob?` Rollback ${p.rollbackJob} queued.`:''}`;
    case 'failed':return `Failed${took}. ${p.error??''}`.trim();
    default:return `${p.state} · ${total} partitions.`;
  }
}
