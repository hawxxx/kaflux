import {useState} from 'react';
import {ChevronDown} from 'lucide-react';
import {bytes} from './api';
import {brokerValues,cv,type Distribution} from './rebalance';

const pageSize=5;
const pct=(v:number|undefined)=>v==null?'—':`${v.toFixed(1)}%`;

/** Collapsible before → after skew for a plan: CV cards and per-broker replicas, five brokers per page. */
export function BalanceEstimate({before,after,measured}:{before:Distribution[];after:Distribution[];measured?:Distribution[]}){
  const [page,setPage]=useState(0);
  const target=measured?.length?measured:after;
  type Row={label:string;from?:number;to?:number};
  const measures:[string,'bytes'|'leaders'|'replicas'][]=[['Data skew · CV','bytes'],['Leader skew · CV','leaders'],['Partition skew · CV','replicas']];
  const rows:Row[]=[];
  for(const [label,key] of measures){const v=brokerValues(before,target,key);if(v.known)rows.push({label,from:cv(v.before),to:cv(v.after)})}
  const replicas=brokerValues(before,target,'replicas');
  const max=Math.max(1,...replicas.before,...replicas.after);
  const avg=replicas.after.reduce((a,b)=>a+b,0)/Math.max(1,replicas.after.length);
  const pages=Math.max(1,Math.ceil(replicas.ids.length/pageSize));
  const shown=replicas.ids.slice(page*pageSize,page*pageSize+pageSize);
  const peek=rows.slice(0,2).map(r=>`${r.label.split(' ')[0].toLowerCase()} ${pct(r.from)} → `);
  const dataBytes=brokerValues(before,target,'bytes');
  return <details className="balance-estimate">
    <summary>
      <span className="be-title">{measured?.length?'Measured':'Estimated'} before → after balance</span>
      <span className="be-peek">{rows.slice(0,2).map((r,i)=><span key={r.label}>{i>0&&' · '}{peek[i]}<span className="be-to">{pct(r.to)}</span></span>)}</span>
      <span className="be-toggle">Details <ChevronDown size={13}/></span>
    </summary>
    <div className="be-body">
      <div className="be-cards">{rows.map(r=><div key={r.label} className="be-card"><span>{r.label}</span><strong><span className="be-from">{pct(r.from)}</span><span className="be-arrow">→</span><span className="be-to">{pct(r.to)}</span></strong></div>)}</div>
      <div className="be-legend"><span><i className="before"/>Before</span><span><i className="after"/>After</span><span><i className="avg"/>Average after</span><span className="be-legend-unit">Replicas per broker</span></div>
      <div className="be-brokers">{shown.map(id=>{const i=replicas.ids.indexOf(id);return <div key={id} className="be-broker">
        <span>broker {id}</span>
        <div className="be-track" aria-label={`Broker ${id}: ${replicas.before[i]} replicas before, ${replicas.after[i]} after`}><div className="before" style={{width:`${replicas.before[i]/max*100}%`}}/><div className="after" style={{width:`${replicas.after[i]/max*100}%`}}/><div className="avg" style={{left:`${avg/max*100}%`}}/></div>
        <span><b>{replicas.before[i]}</b> → {replicas.after[i]}{dataBytes.known?<small> · {bytes(dataBytes.after[i])}</small>:null}</span>
      </div>})}</div>
      {pages>1&&<div className="be-pager"><span>{page*pageSize+1}–{Math.min(replicas.ids.length,(page+1)*pageSize)} of {replicas.ids.length} brokers</span><button type="button" aria-label="Previous brokers" disabled={page===0} onClick={()=>setPage(p=>p-1)}>‹</button><button type="button" aria-label="Next brokers" disabled={page>=pages-1} onClick={()=>setPage(p=>p+1)}>›</button></div>}
      <p className="be-note">{measured?.length?'After values are measured from the cluster on completion; before values come from the approved plan.':'Planner estimates from the approved plan. Once the job completes, these switch to values measured from the cluster.'}</p>
    </div>
  </details>;
}
