import {Shield} from 'lucide-react';
import type {ReactNode} from 'react';
export type ClusterCapabilities={manualReassignmentAllowed:boolean;planningAllowed?:boolean;rebalancingStatus:string;reason:string;kind:string;brokerType:string;observedAt:string};

/**
 * What the reassignment screens may offer. Planning (generate, validate, request a rollback plan)
 * only reads the cluster, so it is available whenever the backend says so. Running a plan changes
 * the cluster and needs manual reassignment to be allowed. Older backends that do not report
 * planningAllowed are treated as before: planning follows permission to run.
 */
export function reassignmentAccess(capabilities?:ClusterCapabilities){
  const canRun=capabilities?.manualReassignmentAllowed===true;
  const canPlan=canRun||capabilities?.planningAllowed===true;
  return {canPlan,canRun,planningOnly:canPlan&&!canRun};
}

// ACTIVE is a warning (AWS owns placement, manual moves are blocked), PAUSED means manual work is
// possible, and anything else is neutral. The tone comes from a fixed list, never from the value.
const tones:Record<string,string>={ACTIVE:'warn',PAUSED:'good',UNKNOWN:'neutral',NOT_APPLICABLE:'neutral'};
const labels:Record<string,string>={ACTIVE:'Active',PAUSED:'Paused',UNKNOWN:'Unknown',NOT_APPLICABLE:'Not applicable'};

/** Intelligent rebalancing status as a colored badge. The status word is kept so it reads without color. */
export function RebalancingStatus({value}:{value?:string}){
  const status=(value??'UNKNOWN').toUpperCase();
  return <span className="rebalancing-status" data-tone={tones[status]??'neutral'} title={`Intelligent rebalancing: ${labels[status]??status}`}>{labels[status]??status}</span>;
}

/** Renders text with every whole-word occurrence of the status shown as the same badge. */
export function withStatusBadges(text:string,status?:string):ReactNode{
  if(!status||!/^[A-Z_]+$/.test(status))return text;
  const word=new RegExp(`\\b(${status})\\b`);
  if(!word.test(text))return text;
  return text.split(word).map((part,i)=>part===status?<RebalancingStatus key={i} value={status}/>:part);
}

export function CapabilityNotice({capabilities,error}:{capabilities?:ClusterCapabilities;error?:Error|null}){
  if(capabilities?.manualReassignmentAllowed)return null;
  const status=capabilities?.rebalancingStatus;
  const active=status==='ACTIVE';
  return <div className="capability-notice" data-tone={active?'warn':undefined} role="status"><Shield size={18}/><div>
    <div className="capability-notice-head"><strong>{active?'AWS owns partition balancing':'Manual reassignment is unavailable'}</strong>{status&&<RebalancingStatus value={status}/>}</div>
    <p>{capabilities?.reason?withStatusBadges(capabilities.reason,status):error?.message??'Waiting for cluster capability preflight. Manual operations remain disabled until capability is known.'}</p>
    {active&&<small>You can generate and validate plans here. Running one stays disabled until intelligent rebalancing is paused through AWS MSK, outside Kaflux.</small>}
  </div></div>;
}
