import {useEffect,useState} from 'react';

/** Whole seconds since `active` became true. Resets to zero when it turns false. */
export function useElapsedSeconds(active:boolean,tickMs=500){
  const [seconds,setSeconds]=useState(0);
  useEffect(()=>{
    if(!active){setSeconds(0);return}
    const started=Date.now();
    const id=window.setInterval(()=>setSeconds(Math.floor((Date.now()-started)/1000)),tickMs);
    return()=>window.clearInterval(id);
  },[active,tickMs]);
  return seconds;
}

/** True only after `active` has stayed true for `delayMs`, so answers that arrive at once never flash an indicator. */
export function useDelayedFlag(active:boolean,delayMs=300){
  const [shown,setShown]=useState(false);
  useEffect(()=>{
    if(!active){setShown(false);return}
    const id=window.setTimeout(()=>setShown(true),delayMs);
    return()=>window.clearTimeout(id);
  },[active,delayMs]);
  return shown;
}

/**
 * Initial load of a list. The API sends no total size, so there is no honest percentage: the bar is
 * indeterminate, the elapsed time shows the request is alive, and a slow request says so.
 */
export function LoadingPanel({label='Retrieving cluster state',slowAfterSeconds=4}:{label?:string;slowAfterSeconds?:number}){
  const seconds=useElapsedSeconds(true);
  return <div className="loading loading-panel" role="status" aria-live="polite">
    <div className="loading-head"><span aria-hidden="true"/><strong>{label}…</strong>{seconds>=1&&<em aria-hidden="true">{seconds}s</em>}</div>
    <div className="loading-bar" role="progressbar" aria-label={label}/>
    {seconds>=slowAfterSeconds&&<p>Still working. Large clusters can take several seconds to respond.</p>}
  </div>;
}

/** Thin bar above a table that already has rows while fresh data is on its way. It reserves its height so nothing shifts. */
export function UpdatingBar({active,label='Updating'}:{active:boolean;label?:string}){
  const shown=useDelayedFlag(active);
  return <div className="updating-bar" data-active={shown}>{shown&&<span className="visually-hidden" role="status">{label}…</span>}</div>;
}
