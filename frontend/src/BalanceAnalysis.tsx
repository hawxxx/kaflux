import type {CSSProperties} from 'react';
import {bytes} from './api';
import {CapacityDetail,CapacityTile,type BrokerCapacity} from './BrokerCapacity';
import {brokerColor,Delta,SkewBadge,type Dimension} from './TopicDistribution';

const format=(d:Dimension,x:number)=>d.id==='bytes'?bytes(x):Number.isInteger(x)?x.toLocaleString():x.toFixed(1);

// Cluster-wide spread of one dimension, as computed by the backend analysis. Brokers keep
// their position color from the topic Distribution view; the marker shows the cluster mean.
export function BalanceAnalysis({dimension:d,capacity}:{dimension:Dimension;capacity?:BrokerCapacity}){
  const max=Math.max(...d.brokers.map(b=>b.value),d.mean,1);
  const mean=`${d.mean/max*100}%`;
  return <>
    <div className="balance-summary">
      <div><span>Distribution skew</span><strong>{(d.coefficientOfVariation*100).toFixed(1)}%</strong><SkewBadge status={d.status}/><small>max {d.maxToMeanRatio.toFixed(2)}× mean · spread {format(d,d.maxMinusMin)}</small></div>
      <div><span>Mean per broker</span><strong>{format(d,d.mean)}</strong><small>{d.id}</small></div>
      <CapacityTile capacity={capacity}/>
    </div>
    <CapacityDetail capacity={capacity}/>
    <div className="broker-bars balance-bars">
      {d.brokers.map((b,i)=><div className="broker-bar-row" key={b.broker}>
        <span><i style={{background:brokerColor(i)}} aria-hidden="true"/>broker-{b.broker}</span>
        <div className="bar-track" style={{'--mean':mean} as CSSProperties} title={`Cluster mean ${format(d,d.mean)}`}><div style={{width:`${b.value/max*100}%`,background:brokerColor(i)}}/></div>
        <strong>{format(d,b.value)}<Delta value={b.percentageDifference}/></strong>
      </div>)}
      <p className="distribution-legend"><span><i className="mean-mark" aria-hidden="true"/>Cluster mean</span></p>
    </div>
  </>
}
