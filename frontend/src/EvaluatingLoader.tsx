import {useEffect,useState} from 'react';
import {Radar} from 'lucide-react';

export type EvalProgress={done:number;total:number};
const fmt=new Intl.NumberFormat();
function eta(seconds:number,{done,total}:EvalProgress){
  if(done<5||seconds<3||done>=total)return '';
  const left=Math.ceil((seconds/done)*(total-done));
  return left<2?'almost done':left<90?`~${left}s left`:`~${Math.ceil(left/60)} min left`;
}
/** Live status of a topic evaluation: real counts from the server once it knows the topic total. */
export function EvaluatingLoader({progress}:{progress?:EvalProgress}){
  const [seconds,setSeconds]=useState(0);
  useEffect(()=>{const t=setInterval(()=>setSeconds(s=>s+1),1000);return()=>clearInterval(t)},[]);
  const known=!!progress&&progress.total>0;
  const pct=known?Math.min(100,Math.round(progress.done/progress.total*100)):0;
  const left=known?eta(seconds,progress):'';
  return <div className="eval-loader" role="status" aria-live="polite">
    <div className="eval-head"><span className="eval-icon"><Radar size={18}/></span>
      <div className="eval-text"><strong>Evaluating topics</strong>
        <span className="muted">{known?`Reading offsets and last writes · ${fmt.format(progress.done)} of ${fmt.format(progress.total)} topics`:'Reading cluster metadata and consumer groups…'}</span></div>
      <div className="eval-meta mono">{known&&<strong>{pct}%</strong>}<span className="muted">{seconds}s{left&&` · ${left}`}</span></div></div>
    <div className={known?'eval-bar eval-determinate':'eval-bar'} role="progressbar" aria-label="Topic evaluation progress" aria-valuemin={0} aria-valuemax={100} aria-valuenow={known?pct:undefined}><span style={known?{width:`${pct}%`}:undefined}/></div>
    <div className="eval-skeleton" aria-hidden="true">{[0,1,2,3,4].map(i=><div key={i} className="eval-row" style={{animationDelay:`${i*120}ms`}}><i style={{width:`${38-i*4}%`}}/><i style={{width:'14%'}}/><i style={{width:'10%'}}/></div>)}</div>
  </div>;
}
