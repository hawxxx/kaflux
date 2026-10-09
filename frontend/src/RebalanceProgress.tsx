import {useState} from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import {Pause,Play,SkipForward,Undo2,X} from 'lucide-react';
import {api,bytes} from './api';
import {BrailleSpinner} from './BrailleSpinner';
import {JobState} from './JobState';
import {duration,type RebalanceJob,type TopicStep} from './rebalance';

type Action='pause'|'resume'|'skip'|'cancel'|'cancel-rollback';

const confirmations:Record<Action,{title:string;body:(job:RebalanceJob,topic?:string)=>string;button:string}>={
  pause:{title:'Pause after this topic',body:(_,t)=>`${t??'The current topic'} keeps moving until it finishes. The job then pauses before the next topic and holds the cluster until you resume, skip or cancel.`,button:'Pause after this topic'},
  resume:{title:'Resume rebalance',body:(_,t)=>`The job continues with ${t??'the next topic'}. A failed topic is retried.`,button:'Resume'},
  skip:{title:'Skip topic',body:(_,t)=>`${t??'The current topic'} is left as it is now and the job continues with the next topic.`,button:'Skip topic'},
  cancel:{title:'Cancel rebalance',body:()=>'Kafka cancels the moves in flight for the current topic. Topics already moved stay moved, and later topics are not started.',button:'Cancel rebalance'},
  'cancel-rollback':{title:'Cancel and roll back',body:j=>`Kafka cancels the moves in flight, then a rollback job restores the original brokers of every partition that moved, newest topic first, with a leader election after each topic. It uses the ${j.throttleBytesPerSec?`${bytes(j.throttleBytesPerSec)}/s throttle`:'same throttle setting'} and starts as soon as cancellation is confirmed.`,button:'Cancel and roll back'},
};

function StepIcon({step}:{step:TopicStep}){
  if(step.state==='moving'||step.state==='electing')return <BrailleSpinner/>;
  return <span className={`step-icon ${step.state}`} aria-hidden="true">{{done:'✔',skipped:'↷',failed:'✖',pending:'○'}[step.state as string]??'○'}</span>;
}

function stepNote(step:TopicStep){
  switch(step.state){
    case 'done':return 'done · leaders elected';
    case 'electing':return 'electing preferred leaders';
    case 'moving':return 'moving';
    case 'failed':return step.error?`failed · ${step.error}`:'failed';
    default:return step.state;
  }
}

function took(step:TopicStep){
  if(!step.startedAt||!step.finishedAt)return '';
  return duration((new Date(step.finishedAt).getTime()-new Date(step.startedAt).getTime())/1000);
}

/** Live progress and controls of an approved rebalance job. */
export function RebalanceProgress({clusterId,job,canAct,onChange}:{clusterId:string;job:RebalanceJob;canAct:boolean;onChange:(job:RebalanceJob)=>void}){
  const [pending,setPending]=useState<Action|null>(null);
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  const [expanded,setExpanded]=useState(false);
  const steps=job.steps??[];
  const index=Math.min(job.currentStep??0,Math.max(steps.length-1,0));
  const current=steps[index];
  const finished=steps.filter(s=>s.state==='done'||s.state==='skipped').length;
  const progress=Math.max(0,Math.min(100,job.progress??0));
  const running=job.state==='running';
  const paused=job.state==='paused';
  const active=running||paused||job.state==='queued';
  const elapsed=job.startedAt?(new Date(job.finishedAt??Date.now()).getTime()-new Date(job.startedAt).getTime())/1000:undefined;
  const visible=expanded?steps:steps.slice(0,Math.max(index+3,5));

  async function run(action:Action){
    setBusy(true);setError('');
    try{
      const path=action==='skip'?'resume':action;
      const r=await api<RebalanceJob>(`/clusters/${encodeURIComponent(clusterId)}/rebalances/${encodeURIComponent(job.id)}/${path}`,{method:'POST',body:JSON.stringify({confirmation:true,planHash:job.planHash,skip:action==='skip'})});
      setPending(null);onChange(r.data);
    }catch(e){setError((e as Error).message)}finally{setBusy(false)}
  }

  const disabled=!canAct||busy||!!job.cancellationRequested;
  return <section className="panel rebalance-progress" aria-label="Rebalance progress">
    <div className="rp-head">
      <JobState state={job.state} warnings={job.warnings} rollbackOf={job.rollbackOf}/>
      <div className="rp-title">
        <h2>{!steps.length?'Rebalance':active?<>Topic {Math.min(finished+1,steps.length)} of {steps.length}{current?<> · <span className="tok-topic">{current.topic}</span></>:null}</>:`${finished} of ${steps.length} topics ${job.state==='completed'?'moved':'finished'}${elapsed!=null?` in ${duration(elapsed)}`:''}`}</h2>
        <p className="muted">{job.throttleBytesPerSec?`Throttle ${bytes(job.throttleBytesPerSec)}/s`:'Unthrottled'}{job.startedAt?` · started ${new Date(job.startedAt).toLocaleTimeString(undefined,{hour12:false})}`:''}{job.approvedBy?` by ${job.approvedBy}`:''}{job.rollbackOf?` · rollback of ${job.rollbackOf}`:''}</p>
      </div>
      {active&&<div className="rp-actions">
        {running&&!job.pauseRequested&&<button className="button tint pause" disabled={disabled} onClick={()=>setPending('pause')}><Pause size={13}/>Pause after this topic</button>}
        {running&&job.pauseRequested&&<span className="rp-pausing"><BrailleSpinner/>Pausing after {current?.topic??'this topic'}</span>}
        {paused&&<button className="button tint go" disabled={disabled} onClick={()=>setPending('resume')}><Play size={13}/>Resume</button>}
        {paused&&current&&<button className="button tint pause" disabled={disabled} onClick={()=>setPending('skip')}><SkipForward size={13}/>Skip topic</button>}
        <button className="button tint danger" disabled={disabled} onClick={()=>setPending('cancel')}><X size={13}/>Cancel</button>
        <button className="button tint danger" disabled={disabled} onClick={()=>setPending('cancel-rollback')}><Undo2 size={13}/>Cancel &amp; roll back</button>
      </div>}
    </div>
    {paused&&job.pauseReason&&<div className="rp-banner" role="status"><Pause size={14}/><span><strong>Paused.</strong> {job.pauseReason}</span></div>}
    {job.cancellationRequested&&active&&<div className="rp-banner danger" role="status"><BrailleSpinner/><span>{job.rollbackRequested?'Canceling, then rolling back the moved partitions.':'Canceling the moves in flight.'}</span></div>}
    {!!job.warnings?.length&&<ul className="rp-warnings">{job.warnings.map((w,i)=><li key={i}>⚠ {w}</li>)}</ul>}
    <div className="rp-progress">
      <div className="rp-top"><span className="rp-percent">{progress}%</span>
        <span className="rp-meta">{running&&job.etaSeconds!=null&&<>ETA <b>{duration(job.etaSeconds)}</b>{job.etaBasis==='topics'&&<span title="Estimated from the average time per topic, because partition sizes are unavailable"> (by topic)</span>} · </>}{running&&job.rateBytesPerSec!=null&&<><b>{bytes(job.rateBytesPerSec)}/s</b> · </>}{elapsed!=null&&<>elapsed <b>{duration(elapsed)}</b></>}</span>
      </div>
      <div className="rp-bar" role="progressbar" aria-label="Rebalance progress" aria-valuemin={0} aria-valuemax={100} aria-valuenow={progress}><div style={{width:`${progress}%`}}/></div>
      <div className="rp-stats">
        <div><span>Topics</span><strong>{finished} / {steps.length}</strong><small>{steps.length-finished} left</small></div>
        <div><span>Partitions</span><strong>{job.partitionsDone??0} / {job.partitionsTotal??job.changes.length}</strong><small>{(job.partitionsTotal??job.changes.length)-(job.partitionsDone??0)} left</small></div>
        <div><span>Data moved</span>{job.bytesTotal!=null?<><strong>{bytes(job.bytesDone??0)} / {bytes(job.bytesTotal)}</strong><small>{bytes(Math.max(0,job.bytesTotal-(job.bytesDone??0)))} left</small></>:<><strong>Unknown</strong><small>Partition sizes unavailable</small></>}</div>
      </div>
    </div>
    {!!steps.length&&<div className="rp-steps"><table><tbody>
      {visible.map((s,i)=>{
        const percent=s.bytes&&s.bytesDone!=null?Math.round(s.bytesDone*100/s.bytes):s.partitions?Math.round(s.partitionsDone*100/s.partitions):0;
        return <tr key={s.topic} className={`step-${s.state}${i===index&&active?' current':''}`}>
          <td className="rp-icon"><StepIcon step={s}/></td>
          <td className="tok-topic">{s.topic}</td>
          <td className="num">{s.partitionsDone}/{s.partitions}</td>
          <td className="num">{s.bytes!=null?bytes(s.bytes):'—'}</td>
          <td>{s.state==='moving'?<><span className="rp-mini"><i style={{width:`${percent}%`}}/></span><span className="num">{percent}%</span></>:<span className="num">{took(s)}</span>}</td>
          <td className={`rp-note ${s.state}`}>{stepNote(s)}</td>
        </tr>;
      })}
      {visible.length<steps.length&&<tr><td/><td colSpan={5}><button type="button" className="link-button" onClick={()=>setExpanded(true)}>+ {steps.length-visible.length} more topics</button></td></tr>}
    </tbody></table></div>}
    <Dialog.Root open={!!pending} onOpenChange={o=>{if(!o){setPending(null);setError('')}}}>
      <Dialog.Portal><Dialog.Overlay className="dialog-overlay"/><Dialog.Content className="dialog">
        {pending&&<>
          <Dialog.Title>{confirmations[pending].title}</Dialog.Title>
          <Dialog.Description>{confirmations[pending].body(job,current?.topic)}</Dialog.Description>
          <Dialog.Close className="dialog-close" aria-label="Close"><X size={18}/></Dialog.Close>
          <div className="review-destination">Job<strong>{job.id}</strong></div>
          {error&&<p role="alert" className="rp-error">{error}</p>}
          <div className="dialog-actions">
            <Dialog.Close asChild><button type="button" className="button">Keep running</button></Dialog.Close>
            <button type="button" className={`button ${pending==='cancel'||pending==='cancel-rollback'?'danger-button':'primary'}`} disabled={busy} onClick={()=>run(pending)}>{busy?'Requesting…':confirmations[pending].button}</button>
          </div>
        </>}
      </Dialog.Content></Dialog.Portal>
    </Dialog.Root>
  </section>;
}
