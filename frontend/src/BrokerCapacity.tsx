import {Fragment} from 'react';
import {bytes} from './api';

export type CapacityStatus='ok'|'above-recommended'|'over-maximum'|'unknown';
export type BrokerCapacity={
  known:boolean;source?:string;label?:string;reference?:string;note?:string;reason?:string;status:CapacityStatus;
  partitionsPerBroker?:{recommended?:number;maximum?:number};diskBytesPerBroker?:number;
  busiest?:{broker:number;replicas:number;percentOfRecommended?:number;percentOfMaximum?:number;status:CapacityStatus};
  medianReplicas?:number;
  brokers:{broker:number;replicas:number;percentOfRecommended?:number;percentOfMaximum?:number;diskBytes?:number;percentOfDisk?:number;status:CapacityStatus}[];
};

const sourceName:Record<string,string>={configured:'Configured by your team','aws-msk':'Documented by AWS for this broker type'};
const statusText:Record<CapacityStatus,string>={ok:'Within limits','above-recommended':'Above the recommended count','over-maximum':'Over the maximum',unknown:'Unknown'};
const n=(v:number)=>v.toLocaleString();
const pct=(v:number)=>`${v<10?v.toFixed(1):Math.round(v)}%`;

// Instance types such as express.m7g.8xlarge have no spaces, so offer line breaks after dots, dashes and underscores
// instead of letting a narrow cell split them in the middle of a word.
function breakable(text:string){return text.split(/(?<=[._-])/).map((part,i)=><Fragment key={i}>{part}<wbr/></Fragment>)}

// The tile headline is the broker type or label, so the operator sees what the limits belong to.
export function capacityHeadline(c?:BrokerCapacity){return c?.known?(c.label||'Capacity known'):'Unknown'}

export function CapacityTile({capacity}:{capacity?:BrokerCapacity}){
  const known=!!capacity?.known;
  const tone=capacity?.status==='over-maximum'?'warn-text':capacity?.status==='above-recommended'?'warn-text':undefined;
  return <div className="capacity-tile">
    <span>Broker capacity</span>
    <strong className={tone} title={capacity?.label}>{breakable(capacityHeadline(capacity))}</strong>
    <small>{known?statusText[capacity!.status]+(capacity!.status==='unknown'&&capacity!.partitionsPerBroker==null?' · no partition limits':''):'No limits set for this cluster'}</small>
  </div>;
}

// Shown under the summary: what the numbers are, where they come from, how close the busiest
// broker is against the recommended and maximum counts, and the median across all brokers.
export function CapacityDetail({capacity}:{capacity?:BrokerCapacity}){
  if(!capacity)return null;
  if(!capacity.known){
    const busiest=capacity.busiest;
    return <div className="capacity-detail" role="note">
      {busiest&&<p>Busiest broker holds <strong>{n(busiest.replicas)}</strong> partition replicas (broker-{busiest.broker}){capacity.medianReplicas!=null?<>, median <strong>{n(capacity.medianReplicas)}</strong></>:null}.</p>}
      <p>{capacity.reason}</p>
    </div>;
  }
  const l=capacity.partitionsPerBroker,busiest=capacity.busiest;
  return <div className="capacity-detail" role="note">
    {l&&<p>Partitions per broker, leaders and followers together:{l.recommended?<> <strong>{n(l.recommended)}</strong> recommended</>:null}{l.recommended&&l.maximum?' · ':null}{l.maximum?<><strong>{n(l.maximum)}</strong> maximum</>:null}.</p>}
    {capacity.diskBytesPerBroker?<p>Log storage per broker: <strong>{bytes(capacity.diskBytesPerBroker)}</strong>.</p>:null}
    {busiest&&l&&<p>Busiest broker (broker-{busiest.broker}) holds <strong>{n(busiest.replicas)}</strong> replicas{busiest.percentOfRecommended!=null?<>, {pct(busiest.percentOfRecommended)} of the recommended count</>:null}{busiest.percentOfMaximum!=null?<>{busiest.percentOfRecommended!=null?' and ':', '}{pct(busiest.percentOfMaximum)} of the maximum</>:null}{capacity.medianReplicas!=null?<>; median across brokers is <strong>{n(capacity.medianReplicas)}</strong></>:null}.</p>}
    <p className="capacity-source">{sourceName[capacity.source??'']??'Source unknown'}{capacity.reference?<> · <a href={capacity.reference} target="_blank" rel="noreferrer noopener">Reference</a></>:null}{capacity.note?` · ${capacity.note}`:''}</p>
  </div>;
}
