import {Check,ChevronDown,RefreshCw} from 'lucide-react';
import {useCallback,useEffect,useId,useRef,useState,type KeyboardEvent} from 'react';

/** Auto refresh choices, in milliseconds. 0 turns auto refresh off. */
export const refreshIntervals=[
  {value:0,label:'Off',long:'Off'},
  {value:5_000,label:'5s',long:'Every 5 seconds'},
  {value:10_000,label:'10s',long:'Every 10 seconds'},
  {value:30_000,label:'30s',long:'Every 30 seconds'},
  {value:60_000,label:'1m',long:'Every minute'},
  {value:300_000,label:'5m',long:'Every 5 minutes'},
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

/**
 * Interval menu: a trigger that shows the current choice and a listbox that follows the ARIA
 * listbox pattern (arrows, Home, End, Enter or Space to choose, Escape or Tab to close).
 */
function IntervalPicker({value,onChange}:{value:number;onChange:(value:number)=>void}){
  const [open,setOpen]=useState(false);
  const [active,setActive]=useState(0);
  const id=useId();
  const root=useRef<HTMLDivElement>(null);
  const trigger=useRef<HTMLButtonElement>(null);
  const list=useRef<HTMLUListElement>(null);
  const selectedIndex=Math.max(0,refreshIntervals.findIndex(x=>x.value===value));
  const current=refreshIntervals[selectedIndex];
  const show=(index=selectedIndex)=>{setActive(index);setOpen(true)};
  const close=(focusTrigger=true)=>{setOpen(false);if(focusTrigger)trigger.current?.focus()};
  const choose=(index:number)=>{onChange(refreshIntervals[index].value);close()};
  useEffect(()=>{if(open)list.current?.focus()},[open]);
  useEffect(()=>{
    if(!open)return;
    const away=(e:PointerEvent)=>{if(!root.current?.contains(e.target as Node))setOpen(false)};
    document.addEventListener('pointerdown',away);
    return()=>document.removeEventListener('pointerdown',away);
  },[open]);
  const onTriggerKey=(e:KeyboardEvent)=>{
    if(['ArrowDown','ArrowUp','Enter',' '].includes(e.key)){e.preventDefault();show(e.key==='ArrowUp'?refreshIntervals.length-1:selectedIndex)}
  };
  const onListKey=(e:KeyboardEvent)=>{
    const last=refreshIntervals.length-1;
    const moves:Record<string,number>={ArrowDown:Math.min(last,active+1),ArrowUp:Math.max(0,active-1),Home:0,End:last};
    if(e.key in moves){e.preventDefault();setActive(moves[e.key])}
    else if(e.key==='Enter'||e.key===' '){e.preventDefault();choose(active)}
    else if(e.key==='Escape'){e.preventDefault();close()}
    else if(e.key==='Tab')close(false);
  };
  return <div className="refresh-picker" ref={root}>
    <button ref={trigger} type="button" className="button refresh-interval" data-active={value>0} aria-label={`Auto refresh: ${current.long}`} aria-haspopup="listbox" aria-expanded={open} aria-controls={open?`${id}-list`:undefined}
      onClick={()=>open?close(false):show()} onKeyDown={onTriggerKey}>
      {value>0&&<i className="refresh-live" aria-hidden="true"/>}
      <span aria-hidden="true">{current.label}</span>
      <ChevronDown size={13} aria-hidden="true" className="refresh-chevron"/>
    </button>
    {open&&<div className="refresh-menu">
      <div className="refresh-menu-title" id={`${id}-title`}>Auto refresh</div>
      <ul ref={list} id={`${id}-list`} role="listbox" tabIndex={-1} aria-labelledby={`${id}-title`} aria-activedescendant={`${id}-opt-${active}`} onKeyDown={onListKey}>
        {refreshIntervals.map((x,i)=><li key={x.value} id={`${id}-opt-${i}`} role="option" aria-selected={i===selectedIndex} data-active={i===active}
          onPointerEnter={()=>setActive(i)} onClick={()=>choose(i)}>
          <span>{x.long}</span>{x.value>0&&<kbd>{x.label}</kbd>}<Check size={13} aria-hidden="true" className="refresh-check"/>
        </li>)}
      </ul>
    </div>}
  </div>;
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
  return <div className="refresh-control" role="group" aria-label="Refresh">
    <button className="button" disabled={refreshing} aria-busy={refreshing} onClick={run}><RefreshCw size={14} aria-hidden="true" className={refreshing?'spin':undefined}/><span className="refresh-label">{refreshing?'Refreshing…':'Refresh'}</span></button>
    <IntervalPicker value={interval} onChange={choose}/>
  </div>;
}
