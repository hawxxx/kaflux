import * as DropdownMenu from '@radix-ui/react-dropdown-menu';
import {Check,ChevronDown,Globe} from 'lucide-react';
import {useCallback,useEffect,useMemo,useState,useSyncExternalStore} from 'react';

/** Minutes east of UTC, or null for the browser's own time zone. */
export type Zone=number|null;

/** Every offset in use around the world, with places that use it as standard time. */
export const zoneOffsets:{offset:number;places:string}[]=[
  {offset:-720,places:'Baker Island'},
  {offset:-660,places:'American Samoa, Niue'},
  {offset:-600,places:'Hawaii'},
  {offset:-570,places:'Marquesas Islands'},
  {offset:-540,places:'Alaska'},
  {offset:-480,places:'US Pacific'},
  {offset:-420,places:'US Mountain, Arizona'},
  {offset:-360,places:'US Central, Mexico City'},
  {offset:-300,places:'US Eastern, Bogotá, Lima'},
  {offset:-240,places:'Atlantic, Caracas, La Paz'},
  {offset:-210,places:'Newfoundland'},
  {offset:-180,places:'São Paulo, Buenos Aires'},
  {offset:-120,places:'South Georgia'},
  {offset:-60,places:'Azores, Cape Verde'},
  {offset:0,places:'Coordinated Universal Time'},
  {offset:60,places:'Central Europe, Lagos'},
  {offset:120,places:'Eastern Europe, Cairo, Johannesburg'},
  {offset:180,places:'Moscow, Istanbul, Riyadh'},
  {offset:210,places:'Tehran'},
  {offset:240,places:'Dubai, Baku'},
  {offset:270,places:'Kabul'},
  {offset:300,places:'Karachi, Tashkent'},
  {offset:330,places:'India, Sri Lanka'},
  {offset:345,places:'Nepal'},
  {offset:360,places:'Dhaka, Bhutan'},
  {offset:390,places:'Yangon'},
  {offset:420,places:'Bangkok, Jakarta'},
  {offset:480,places:'Singapore, Beijing, Perth'},
  {offset:525,places:'Eucla'},
  {offset:540,places:'Tokyo, Seoul'},
  {offset:570,places:'Darwin, Adelaide'},
  {offset:600,places:'Sydney, Brisbane'},
  {offset:630,places:'Lord Howe Island'},
  {offset:660,places:'Solomon Islands, Nouméa'},
  {offset:720,places:'Auckland, Fiji'},
  {offset:765,places:'Chatham Islands'},
  {offset:780,places:'Tonga, Samoa'},
  {offset:840,places:'Line Islands'},
];

const storageKey='kaflux-time-zone';

/** UTC, UTC+05:30 or UTC-03:00. */
export function offsetLabel(offset:number):string{
  if(!offset)return 'UTC';
  const abs=Math.abs(offset);
  return `UTC${offset<0?'-':'+'}${String(Math.floor(abs/60)).padStart(2,'0')}:${String(abs%60).padStart(2,'0')}`;
}
const browserOffset=()=>-new Date().getTimezoneOffset();

/** Reads the saved zone. Anything that is not one of the offered offsets means the browser zone. */
export function loadZone(storage?:Pick<Storage,'getItem'>):Zone{
  try{
    const raw=(storage??globalThis.localStorage)?.getItem(storageKey);
    if(raw==null||raw==='')return null;
    const value=Number(raw);
    return zoneOffsets.some(x=>x.offset===value)?value:null;
  }catch{return null}
}

// One choice for the whole app, shared by every component that formats or reads a time.
let current:Zone|undefined;
const listeners=new Set<()=>void>();
const snapshot=()=>current===undefined?(current=loadZone()):current;
const subscribe=(listener:()=>void)=>{listeners.add(listener);return()=>{listeners.delete(listener)}};
export function setZone(zone:Zone){
  current=zone;
  try{if(zone==null)localStorage.removeItem(storageKey);else localStorage.setItem(storageKey,String(zone))}catch{/* private mode */}
  listeners.forEach(l=>l());
}

type Style='dateTime'|'time';
/**
 * Formats a time in the zone. The browser zone keeps the plain locale format; a chosen offset is
 * shown as wall clock time at that offset, and full dates name the offset so it is never ambiguous.
 */
export function formatInZone(value:string|number|Date,zone:Zone,style:Style='dateTime'):string{
  const date=new Date(value);
  if(zone==null)return style==='time'?date.toLocaleTimeString():date.toLocaleString();
  const shifted=new Date(date.getTime()+zone*60_000);
  if(!Number.isFinite(shifted.getTime()))return date.toLocaleString();
  return style==='time'?shifted.toLocaleTimeString(undefined,{timeZone:'UTC'}):`${shifted.toLocaleString(undefined,{timeZone:'UTC'})} ${offsetLabel(zone)}`;
}

/** Reads a datetime-local value (2026-10-08T14:30) as wall clock time in the zone. */
export function parseInZone(value:string,zone:Zone):Date{
  if(zone==null)return new Date(value);
  const m=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/.exec(value);
  if(!m)return new Date(NaN);
  const [,y,mo,d,h,mi,s='0',ms='0']=m;
  return new Date(Date.UTC(+y,+mo-1,+d,+h,+mi,+s,+ms.padEnd(3,'0'))-zone*60_000);
}

/** The selected zone and helpers bound to it. Components re-render when the choice changes. */
export function useTimeZone(){
  const zone=useSyncExternalStore(subscribe,snapshot,snapshot);
  return useMemo(()=>({
    zone,
    label:offsetLabel(zone??browserOffset()),
    dateTime:(v:string|number|Date)=>formatInZone(v,zone,'dateTime'),
    time:(v:string|number|Date)=>formatInZone(v,zone,'time'),
    parse:(v:string)=>parseInZone(v,zone),
    /**
     * uPlot tzDate option: a Date whose local fields read as the wall clock at the offset, so axis
     * ticks show zone time. Undefined keeps uPlot's browser zone.
     */
    chartDate:zone==null?undefined:(ts:number)=>{const d=new Date(ts*1000+zone*60_000);return new Date(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate(),d.getUTCHours(),d.getUTCMinutes(),d.getUTCSeconds(),d.getUTCMilliseconds())},
  }),[zone]);
}

const clock=(zone:Zone,now:number)=>{
  const shifted=zone==null?new Date(now):new Date(now+zone*60_000);
  return shifted.toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit',...(zone==null?{}:{timeZone:'UTC'})});
};

/** Ticks once a minute, on the minute, so clocks never show a stale time. */
function useMinute(){
  const [now,setNow]=useState(()=>Date.now());
  useEffect(()=>{
    let timer:number;
    const schedule=()=>{timer=window.setTimeout(()=>{setNow(Date.now());schedule()},60_000-Date.now()%60_000)};
    schedule();
    return()=>window.clearTimeout(timer);
  },[]);
  return now;
}

/** Top bar menu that chooses the time zone used for every time shown or entered in the app. */
export function TimeZonePicker(){
  const {zone,label}=useTimeZone();
  const now=useMinute();
  const local=browserOffset();
  // Start on the chosen offset, scrolled into view, rather than at the top of a long list. The frame
  // lets the menu take focus first; the callback is stable so the minute tick does not refocus.
  const focusChecked=useCallback((menu:HTMLDivElement|null)=>{
    if(menu)requestAnimationFrame(()=>menu.querySelector<HTMLElement>('.tz-scroll [data-state=checked]')?.focus());
  },[]);
  return <DropdownMenu.Root modal={false}>
    <DropdownMenu.Trigger className="button tz-trigger" aria-label={`Time zone: ${zone==null?`browser, ${label}`:label}`}>
      <Globe size={14} aria-hidden="true"/>
      <span className="tz-clock" aria-hidden="true">{clock(zone,now)}</span>
      <span className="tz-label" aria-hidden="true">{label}</span>
      <ChevronDown size={13} aria-hidden="true" className="tz-chevron"/>
    </DropdownMenu.Trigger>
    <DropdownMenu.Portal><DropdownMenu.Content className="tz-menu" align="end" sideOffset={6} collisionPadding={8} ref={focusChecked}>
      <DropdownMenu.Label className="tz-menu-title">Time zone</DropdownMenu.Label>
      <DropdownMenu.RadioGroup value={zone==null?'local':String(zone)} onValueChange={v=>setZone(v==='local'?null:Number(v))}>
        <ZoneItem value="local" name="Browser time" detail={offsetLabel(local)} time={clock(null,now)}/>
        <ZoneItem value="0" name="UTC" detail="Coordinated Universal Time" time={clock(0,now)}/>
        <DropdownMenu.Separator className="tz-separator"/>
        <div className="tz-scroll">
          {zoneOffsets.filter(x=>x.offset!==0).map(x=><ZoneItem key={x.offset} value={String(x.offset)} name={offsetLabel(x.offset)} detail={x.places} time={clock(x.offset,now)}/>)}
        </div>
      </DropdownMenu.RadioGroup>
      <p className="tz-note">Fixed offsets, no daylight saving. Applies to message times, charts and timestamp fields.</p>
    </DropdownMenu.Content></DropdownMenu.Portal>
  </DropdownMenu.Root>;
}

function ZoneItem({value,name,detail,time}:{value:string;name:string;detail:string;time:string}){
  return <DropdownMenu.RadioItem className="tz-item" value={value} textValue={name}>
    <span className="tz-item-text"><strong>{name}</strong><small>{detail}</small></span>
    <kbd>{time}</kbd>
    <span className="tz-check"><DropdownMenu.ItemIndicator><Check size={13}/></DropdownMenu.ItemIndicator></span>
  </DropdownMenu.RadioItem>;
}
