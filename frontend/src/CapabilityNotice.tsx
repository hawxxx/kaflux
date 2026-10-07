import {Shield} from 'lucide-react';
import type {ReactNode} from 'react';
export type ClusterCapabilities={manualReassignmentAllowed:boolean;rebalancingStatus:string;reason:string;kind:string;brokerType:string;observedAt:string};

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
    {active&&<small>Pause intelligent rebalancing through AWS MSK outside Kaflux before requesting a manual plan. Read-only distribution analysis remains available.</small>}
  </div></div>;
}
