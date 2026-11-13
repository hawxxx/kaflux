import {Minus,Plus} from 'lucide-react';
import {bytes} from './api';

const step=1024*1024;
/** The stepper never goes below one step: a few bytes per second would stall every move. */
const floor=step;

/** The plan form's replication throttle: a switch plus a rate, locked on when the cluster requires one. */
export function ThrottleField({enabled,required,value,onEnabled,onValue}:{enabled:boolean;required:boolean;value:string;onEnabled:(on:boolean)=>void;onValue:(v:string)=>void}){
  const on=enabled||required;
  return <div className="throttle-field">
    <label className="switch-label"><input type="checkbox" role="switch" checked={on} disabled={required} onChange={e=>onEnabled(e.target.checked)}/>Replication throttle</label>
    <div className="throttle-stepper">
      <button type="button" aria-label="Decrease throttle by 1 MiB/s" disabled={!on||Number(value)<=floor} onClick={()=>onValue(String(Math.max(floor,(Number(value)||0)-step)))}><Minus size={13} aria-hidden/></button>
      <input type="number" min="1" aria-label="Throttle bytes per second" value={value} disabled={!on} onChange={e=>onValue(e.target.value)}/>
      <button type="button" aria-label="Increase throttle by 1 MiB/s" disabled={!on} onClick={()=>onValue(String((Number(value)||0)+step))}><Plus size={13} aria-hidden/></button>
    </div>
    <small>{required?'Required on this cluster':on?`${bytes(Number(value)||0)}/s per broker`:'Unthrottled: moves run at full speed'}</small>
  </div>;
}

/** The rate sent with a new plan; zero means unthrottled. */
export const throttleRate=(enabled:boolean,required:boolean,value:string)=>enabled||required?Number(value):0;
