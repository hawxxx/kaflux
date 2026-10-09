import type {ThrottlePlan} from './ThrottleControl';

export type StepState='pending'|'moving'|'electing'|'done'|'skipped'|'failed';
export type TopicStep={topic:string;state:StepState;partitions:number;partitionsDone:number;bytes?:number;bytesDone?:number;startedAt?:string;finishedAt?:string;error?:string};
export type JobEvent={seq:number;at:string;level:'info'|'warn'|'error';message:string};
export type Change={topic:string;partition:number;before:number[];after:number[];finishedAt?:string};
export type RebalanceJob=ThrottlePlan&{
  changes:Change[];before:unknown;after:unknown;createdAt:string;progress?:number;
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
