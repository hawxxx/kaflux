import {Shield} from 'lucide-react';
export type ClusterCapabilities={manualReassignmentAllowed:boolean;rebalancingStatus:string;reason:string;kind:string;brokerType:string;observedAt:string};
export function CapabilityNotice({capabilities,error}:{capabilities?:ClusterCapabilities;error?:Error|null}){
  if(capabilities?.manualReassignmentAllowed)return null;
  return <div className="capability-notice" role="status"><Shield size={18}/><div><strong>{capabilities?.rebalancingStatus==='ACTIVE'?'AWS owns partition balancing':'Manual reassignment is unavailable'}</strong><p>{capabilities?.reason??error?.message??'Waiting for cluster capability preflight. Manual operations remain disabled until capability is known.'}</p>{capabilities?.rebalancingStatus==='ACTIVE'&&<small>Pause intelligent rebalancing through AWS MSK outside Kaflux before requesting a manual plan. Read-only distribution analysis remains available.</small>}</div></div>
}
