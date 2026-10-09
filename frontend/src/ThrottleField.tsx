import {bytes} from './api';

/** The plan form's replication throttle: a switch plus a rate, locked on when the cluster requires one. */
export function ThrottleField({enabled,required,value,onEnabled,onValue}:{enabled:boolean;required:boolean;value:string;onEnabled:(on:boolean)=>void;onValue:(v:string)=>void}){
  const on=enabled||required;
  return <div className="throttle-field">
    <label className="throttle-switch"><input type="checkbox" role="switch" checked={on} disabled={required} onChange={e=>onEnabled(e.target.checked)}/>Replication throttle</label>
    <input type="number" min="1" aria-label="Throttle bytes per second" value={value} disabled={!on} onChange={e=>onValue(e.target.value)}/>
    <small>{required?'Required on this cluster':on?`${bytes(Number(value)||0)}/s per broker`:'Unthrottled: moves run at full speed'}</small>
  </div>;
}

/** The rate sent with a new plan; zero means unthrottled. */
export const throttleRate=(enabled:boolean,required:boolean,value:string)=>enabled||required?Number(value):0;
