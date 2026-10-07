import {Check,ChevronDown,RefreshCw} from 'lucide-react';
import {createContext,useCallback,useContext,useEffect,useId,useMemo,useRef,useState,type KeyboardEvent,type ReactNode} from 'react';

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

/** Short text for any interval, such as 30s or 2m. */
export function formatInterval(ms:number):string{
  if(!ms)return 'Off';
  return ms%60_000===0?`${ms/60_000}m`:`${Math.round(ms/1000)}s`;
}
function longInterval(ms:number):string{
  const offered=refreshIntervals.find(x=>x.value===ms)?.long;
  if(offered)return offered;
  if(!ms)return 'Off';
  const [n,unit]=ms%60_000===0?[ms/60_000,'minute']:[Math.round(ms/1000),'second'];
  return `Every ${n===1?'':`${n} `}${unit}${n===1?'':'s'}`;
}

/**
 * Reads the saved choice. null means nothing was chosen, so each page keeps its own default;
 * 0 is a deliberate Off. Anything that is not one of the offered values counts as no choice.
 */
export function loadInterval(storage?:Pick<Storage,'getItem'>):number|null{
  try{
    // Reading localStorage itself can throw when storage is blocked, so it stays inside the try.
    const raw=(storage??globalThis.localStorage)?.getItem(storageKey);
    if(raw==null||raw==='')return null;
    const value=Number(raw);
    return refreshIntervals.some(x=>x.value===value)?value:null;
  }catch{return null}
}

type Kind='steady'|'essential';
type RefreshState={
  /** The user's choice: null follows each page's own refresh, 0 is Off, other values are in ms. */
  override:number|null;
  setOverride:(value:number|null)=>void;
  /** Registers a refresh that is on screen, and returns the function that removes it. */
  register:(id:string,ms:number,kind:Kind)=>()=>void;
  /** The shortest steady refresh of what is on screen, 0 when nothing refreshes by itself. */
  pageDefault:number;
  /** The shortest live update that continues whatever is chosen (job progress, live tail), 0 if none. */
  essential:number;
};
const RefreshContext=createContext<RefreshState|null>(null);

export function RefreshProvider({children}:{children:ReactNode}){
  const [override,setOverrideState]=useState<number|null>(()=>loadInterval());
  // Only the two shortest values are state, so the provider re-renders when they change and not
  // each time a query on screen registers or leaves.
  const [pageDefault,setPageDefault]=useState(0);
  const [essential,setEssential]=useState(0);
  const registry=useRef(new Map<string,{ms:number;kind:Kind}>());
  const setOverride=useCallback((value:number|null)=>{
    setOverrideState(value);
    try{if(value==null)localStorage.removeItem(storageKey);else localStorage.setItem(storageKey,String(value))}catch{/* private mode */}
  },[]);
  const recompute=useCallback(()=>{
    const shortest=(kind:Kind)=>Math.min(Infinity,...[...registry.current.values()].filter(x=>x.kind===kind).map(x=>x.ms));
    const steady=shortest('steady'),live=shortest('essential');
    setPageDefault(Number.isFinite(steady)?steady:0);
    setEssential(Number.isFinite(live)?live:0);
  },[]);
  const register=useCallback((id:string,ms:number,kind:Kind)=>{
    registry.current.set(id,{ms,kind});recompute();
    return()=>{registry.current.delete(id);recompute()};
  },[recompute]);
  const value=useMemo(()=>({override,setOverride,register,pageDefault,essential}),[override,setOverride,register,pageDefault,essential]);
  return <RefreshContext.Provider value={value}>{children}</RefreshContext.Provider>;
}

/**
 * The interval a query should poll at for its steady refresh. Without a choice from the user it is
 * the query's own default, which is also announced to the picker so the picker shows the truth.
 * Once the user picks an interval (or Off) the picker's single timer takes over and this returns
 * false, so nothing keeps refreshing on a second, hidden schedule.
 */
export function useSteadyInterval(defaultMs:number,active=true):number|false{
  const ctx=useContext(RefreshContext);
  const id=useId();
  const register=ctx?.register;
  useEffect(()=>{
    if(!register||!active)return;
    return register(id,defaultMs,'steady');
  },[register,id,defaultMs,active]);
  return !ctx||ctx.override===null?defaultMs:false;
}

/**
 * Polling that must go on whatever interval is chosen, such as the progress of a running
 * reassignment or a live tail. It is not controlled by the picker, which says so in its menu.
 */
export function useEssentialInterval(ms:number,active=true):number|undefined{
  const ctx=useContext(RefreshContext);
  const id=useId();
  const register=ctx?.register;
  useEffect(()=>{
    if(!register||!active)return;
    return register(id,ms,'essential');
  },[register,id,ms,active]);
  return active?ms:undefined;
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

type Option={key:string;value:number|null;label:string;long:string};

/**
 * Interval menu: a trigger that shows the interval in effect and a listbox that follows the ARIA
 * listbox pattern (arrows, Home, End, Enter or Space to choose, Escape or Tab to close).
 */
function IntervalPicker({override,pageDefault,essential,onChange}:{override:number|null;pageDefault:number;essential:number;onChange:(value:number|null)=>void}){
  // With no choice made, the label is what is really refreshing on screen: page refreshes and live
  // updates alike, whichever is shortest.
  const inUse=[pageDefault,essential].filter(x=>x>0);
  const defaultShown=inUse.length?Math.min(...inUse):0;
  const options:Option[]=[
    {key:'default',value:null,label:formatInterval(defaultShown),long:'Page default'},
    ...refreshIntervals.map(x=>({key:String(x.value),value:x.value,label:x.label,long:x.long})),
  ];
  const [open,setOpen]=useState(false);
  const [active,setActive]=useState(0);
  const id=useId();
  const root=useRef<HTMLDivElement>(null);
  const trigger=useRef<HTMLButtonElement>(null);
  const list=useRef<HTMLUListElement>(null);
  const selectedIndex=Math.max(0,options.findIndex(x=>x.value===override));
  const effective=override??defaultShown;
  const show=(index=selectedIndex)=>{setActive(index);setOpen(true)};
  const close=(focusTrigger=true)=>{setOpen(false);if(focusTrigger)trigger.current?.focus()};
  const choose=(index:number)=>{onChange(options[index].value);close()};
  useEffect(()=>{if(open)list.current?.focus()},[open]);
  useEffect(()=>{
    if(!open)return;
    const away=(e:PointerEvent)=>{if(!root.current?.contains(e.target as Node))setOpen(false)};
    document.addEventListener('pointerdown',away);
    return()=>document.removeEventListener('pointerdown',away);
  },[open]);
  const onTriggerKey=(e:KeyboardEvent)=>{
    if(['ArrowDown','ArrowUp','Enter',' '].includes(e.key)){e.preventDefault();show(e.key==='ArrowUp'?options.length-1:selectedIndex)}
  };
  const onListKey=(e:KeyboardEvent)=>{
    const last=options.length-1;
    const moves:Record<string,number>={ArrowDown:Math.min(last,active+1),ArrowUp:Math.max(0,active-1),Home:0,End:last};
    if(e.key in moves){e.preventDefault();setActive(moves[e.key])}
    else if(e.key==='Enter'||e.key===' '){e.preventDefault();choose(active)}
    else if(e.key==='Escape'){e.preventDefault();close()}
    else if(e.key==='Tab')close(false);
  };
  const stillUpdating=override!==null&&essential>0?`. Live job status and live tail still update every ${formatInterval(essential)}`:'';
  const state=(override===null?`${longInterval(effective)}, page default`:longInterval(effective))+stillUpdating;
  return <div className="refresh-picker" ref={root}>
    <button ref={trigger} type="button" className="button refresh-interval" data-active={effective>0} aria-label={`Auto refresh: ${state}`} aria-haspopup="listbox" aria-expanded={open} aria-controls={open?`${id}-list`:undefined}
      onClick={()=>open?close(false):show()} onKeyDown={onTriggerKey}>
      {effective>0&&<i className="refresh-live" aria-hidden="true"/>}
      <span aria-hidden="true">{formatInterval(effective)}</span>
      <ChevronDown size={13} aria-hidden="true" className="refresh-chevron"/>
    </button>
    {open&&<div className="refresh-menu">
      <div className="refresh-menu-title" id={`${id}-title`}>Auto refresh</div>
      <ul ref={list} id={`${id}-list`} role="listbox" tabIndex={-1} aria-labelledby={`${id}-title`} aria-activedescendant={`${id}-opt-${active}`} onKeyDown={onListKey}>
        {options.map((x,i)=><li key={x.key} id={`${id}-opt-${i}`} role="option" aria-selected={i===selectedIndex} data-active={i===active}
          onPointerEnter={()=>setActive(i)} onClick={()=>choose(i)}>
          <span>{x.long}</span>{(x.value===null||x.value>0)&&<kbd>{x.label}</kbd>}<Check size={13} aria-hidden="true" className="refresh-check"/>
        </li>)}
      </ul>
      <p className="refresh-menu-note">
        {override===null?'Each page refreshes on its own schedule. Choose a value to use one schedule everywhere.':'One schedule now applies to every page.'}
        {essential>0&&` Live job status and live tail keep updating every ${formatInterval(essential)}.`}
      </p>
    </div>}
  </div>;
}

/**
 * Grafana style refresh control: a refresh button joined to an auto refresh picker. The manual
 * button and the timer share one in-flight guard, so a timed refresh never overlaps a manual one.
 */
export function RefreshControl({onRefresh}:{onRefresh:()=>Promise<unknown>}){
  const ctx=useContext(RefreshContext);
  if(!ctx)throw new Error('RefreshControl must be rendered inside RefreshProvider');
  const {override,setOverride,pageDefault,essential}=ctx;
  const [refreshing,setRefreshing]=useState(false);
  const busy=useRef(false);
  const run=useCallback(async()=>{
    if(busy.current)return;
    busy.current=true;setRefreshing(true);
    try{await onRefresh()}finally{busy.current=false;setRefreshing(false)}
  },[onRefresh]);
  // The timer belongs to an explicit choice. Without one, pages poll on their own defaults.
  useAutoRefresh(override&&override>0?override:0,run);
  return <div className="refresh-control" role="group" aria-label="Refresh">
    <button className="button" disabled={refreshing} aria-busy={refreshing} onClick={run}><RefreshCw size={14} aria-hidden="true" className={refreshing?'spin':undefined}/><span className="refresh-label">{refreshing?'Refreshing…':'Refresh'}</span></button>
    <IntervalPicker override={override} pageDefault={pageDefault} essential={essential} onChange={setOverride}/>
  </div>;
}
