import {useEffect,useState} from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import {api,bytes} from './api';
import './ThrottleControl.css';

export type ThrottlePlan={id:string;state:string;planHash:string;topics:string[];throttleBytesPerSec?:number;throttleRequest?:{revision:string;bytesPerSec:number};throttleError?:string;cancellationRequested?:boolean;cleanupPending?:boolean};
export function ThrottleControl({clusterId,plan,canWrite,allowed,onChange}:{clusterId:string;plan:ThrottlePlan;canWrite:boolean|undefined;allowed:boolean;onChange:()=>void}){
  const [open,setOpen]=useState(false),[rate,setRate]=useState(''),[confirmation,setConfirmation]=useState(''),[busy,setBusy]=useState(false),[error,setError]=useState('');
  const [submitted,setSubmitted]=useState<ThrottlePlan['throttleRequest']>();
  useEffect(()=>{setSubmitted(undefined)},[plan.id,plan.state,plan.throttleBytesPerSec,plan.throttleRequest?.revision]);
  useEffect(()=>{setOpen(false);setError('');setConfirmation('')},[plan.id]);
  const pending=plan.throttleRequest??submitted;
  const disabled=!canWrite||!allowed||plan.state!=='running'||plan.cancellationRequested||plan.cleanupPending||!!pending;
  const value=Number(rate),valid=Number.isSafeInteger(value)&&value>=1&&value<=1e12&&confirmation===plan.id;
  async function submit(event:React.FormEvent){event.preventDefault();if(!valid||disabled||busy)return;setBusy(true);setError('');try{
    const response=await api<ThrottlePlan>(`/clusters/${encodeURIComponent(clusterId)}/rebalances/${encodeURIComponent(plan.id)}/throttle`,{method:'POST',body:JSON.stringify({confirmation:true,planHash:plan.planHash,bytesPerSec:value})});
    setSubmitted(response.data.throttleRequest);setOpen(false);onChange();
  }catch(e){setError((e as Error).message)}finally{setBusy(false)}}
  return <div className="throttle-control"><p>Current job throttle: {plan.throttleBytesPerSec==null?'Unavailable':`${bytes(plan.throttleBytesPerSec)}/s`}</p>
    {pending&&<p role="status">Requested: {bytes(pending.bytesPerSec)}/s — awaiting worker verification</p>}
    {plan.throttleError&&<p role="alert">{plan.throttleError}</p>}
    <Dialog.Root open={open} onOpenChange={next=>{setOpen(next);if(next){setRate(String(plan.throttleBytesPerSec??10485760));setConfirmation('');setError('')}}}>
      <Dialog.Trigger asChild><button className="button" disabled={!!disabled||busy}>Change throttle</button></Dialog.Trigger>
      <Dialog.Portal><Dialog.Overlay className="dialog-overlay"/><Dialog.Content className="dialog throttle-dialog">
        <Dialog.Title>Change reassignment throttle</Dialog.Title><Dialog.Description>Request a replication rate for {plan.topics.join(', ')}. The worker verifies Kafka configuration before marking the rate applied. Zero does not pause reassignment.</Dialog.Description>
        <form onSubmit={submit}><label>New rate · bytes per second<input type="number" min="1" max="1000000000000" step="1" value={rate} onChange={event=>setRate(event.target.value)}/></label>
          <p className="mono">Job {plan.id}</p><label>Confirm job ID<input autoComplete="off" value={confirmation} onChange={event=>setConfirmation(event.target.value)}/></label>
          {error&&<p role="alert">{error}</p>}
          <div className="dialog-actions"><Dialog.Close asChild><button type="button" className="button">Cancel</button></Dialog.Close><button type="submit" className="button primary" disabled={!valid||!!disabled||busy}>{busy?'Requesting…':'Request throttle change'}</button></div>
        </form>
      </Dialog.Content></Dialog.Portal>
    </Dialog.Root>
  </div>
}
