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

const isWordChar=(c?:string)=>c!==undefined&&(c==='_'||(c>='0'&&c<='9')||(c>='A'&&c<='Z')||(c>='a'&&c<='z'));
const isStatusToken=(value:string)=>value.length>0&&[...value].every(c=>c==='_'||(c>='A'&&c<='Z'));

/** Renders text with every whole-word occurrence of the status shown as the same badge. */
export function withStatusBadges(text:string,status?:string):ReactNode{
  if(!status||!isStatusToken(status))return text;
  const out:ReactNode[]=[];
  let last=0;
  for(let at=text.indexOf(status);at!==-1;at=text.indexOf(status,at+status.length)){
    const end=at+status.length;
    if(isWordChar(text[at-1])||isWordChar(text[end]))continue;
    if(at>last)out.push(text.slice(last,at));
    out.push(<RebalancingStatus key={at} value={status}/>);
    last=end;
  }
  if(!out.length)return text;
  if(last<text.length)out.push(text.slice(last));
  return out;
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
