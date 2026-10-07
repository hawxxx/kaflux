import {RefreshCw} from 'lucide-react';
import {useCallback,useEffect,useRef,useState} from 'react';

/** Auto refresh choices, in milliseconds. 0 turns auto refresh off. */
export const refreshIntervals=[
  {value:0,label:'Off'},
  {value:5_000,label:'5s'},
  {value:10_000,label:'10s'},
  {value:30_000,label:'30s'},
  {value:60_000,label:'1m'},
  {value:300_000,label:'5m'},
] as const;

const storageKey='kaflux-refresh-interval';

/** Reads the saved interval, accepting only one of the offered values. */
export function loadInterval(storage?:Pick<Storage,'getItem'>):number{
  try{
    // Reading localStorage itself can throw when storage is blocked, so it stays inside the try.
    const raw=Number((storage??globalThis.localStorage)?.getItem(storageKey));
    return refreshIntervals.some(x=>x.value===raw)?raw:0;
  }catch{return 0}
}

/**
 * Runs `refresh` every `interval` ms. The next run is scheduled only after the previous one
 * settles, so slow responses never pile up, and nothing runs while the tab is hidden. When the
 * tab becomes visible again after the interval has passed, it refreshes at once.
 */
export function useAutoRefresh(interval:number,refresh:()=>Promise<unknown>){
  const refreshRef=useRef(refresh);
  refreshRef.current=refresh;
  useEffect(()=>{
    if(!interval)return;
    let timer:number|undefined;
    let cancelled=false;
    let last=Date.now();
    const schedule=(delay:number)=>{window.clearTimeout(timer);timer=window.setTimeout(tick,Math.max(0,delay))};
    const tick=async()=>{
      if(cancelled)return;
      if(document.visibilityState==='hidden')return; // resumed by the visibility listener
      try{await refreshRef.current()}catch{/* the queries report their own errors */}
      last=Date.now();
      if(!cancelled)schedule(interval);
    };
    const onVisible=()=>{if(document.visibilityState==='visible'&&!cancelled)schedule(interval-(Date.now()-last))};
    document.addEventListener('visibilitychange',onVisible);
    schedule(interval);
    return()=>{cancelled=true;window.clearTimeout(timer);document.removeEventListener('visibilitychange',onVisible)};
  },[interval]);
}

/** Grafana style refresh control: a refresh button joined to an auto refresh interval picker. */
export function RefreshControl({onRefresh}:{onRefresh:()=>Promise<unknown>}){
  const [interval,setIntervalValue]=useState(()=>loadInterval());
  const [refreshing,setRefreshing]=useState(false);
  const busy=useRef(false);
  const run=useCallback(async()=>{
    if(busy.current)return;
    busy.current=true;setRefreshing(true);
    try{await onRefresh()}finally{busy.current=false;setRefreshing(false)}
  },[onRefresh]);
  useAutoRefresh(interval,run);
  const choose=(value:number)=>{setIntervalValue(value);try{localStorage.setItem(storageKey,String(value))}catch{/* private mode */}};
  const label=refreshIntervals.find(x=>x.value===interval)?.label??'Off';
  return <div className="refresh-control" role="group" aria-label="Refresh">
    <button className="button" disabled={refreshing} aria-busy={refreshing} onClick={run}><RefreshCw size={14} className={refreshing?'spin':undefined}/>{refreshing?'Refreshing…':'Refresh'}</button>
    <label className="button refresh-interval" data-active={interval>0} title={interval?`Refreshing every ${label}`:'Auto refresh is off'}>
      <span aria-hidden="true">{label}</span>
      <select aria-label="Auto refresh interval" value={interval} onChange={e=>choose(Number(e.target.value))}>
        {refreshIntervals.map(x=><option key={x.value} value={x.value}>{x.value?`Every ${x.label}`:'Off'}</option>)}
      </select>
    </label>
  </div>;
}
