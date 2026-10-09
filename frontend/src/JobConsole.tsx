import {useEffect,useMemo,useRef,useState} from 'react';
import {ArrowDownToLine,FileText,ShieldCheck,Table2} from 'lucide-react';
import {api} from './api';
import {BrailleSpinner} from './BrailleSpinner';
import type {JobEvent,RebalanceJob} from './rebalance';

const glyphs:Record<string,string>={'✔':'ok','▶':'go','⚠':'warn','✖':'bad','⇄':'lit','⏸':'warn','↩':'warn'};
const escape=(s:string)=>s.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');

type Token={text:string;kind?:'topic'|'num'|'key'|'dur'|'lit'};

/** Splits a log line into colored tokens: topics, sizes and counts, broker lists, durations. */
export function tokenize(message:string,topics:string[]):Token[]{
  const names=[...topics].sort((a,b)=>b.length-a.length).map(escape);
  const parts=[
    names.length?`(?<topic>${names.join('|')})(?![\\w.-])`:'',
    '(?<key>\\[[\\d,]*\\])',
    '(?<dur>\\b\\d+h\\d{2}m\\b|\\b\\d+m\\d{2}s\\b|\\b\\d+s\\b)',
    '(?<num>\\b\\d+(?:\\.\\d+)?(?:\\/\\d+(?:\\.\\d+)?)?(?: ?(?:B|KiB|MiB|GiB|TiB|PiB)(?:\\/s)?|%)?)',
    '(?<lit>\\bPREFERRED\\b)',
  ].filter(Boolean);
  const re=new RegExp(parts.join('|'),'g');
  const out:Token[]=[];let last=0;
  for(const m of message.matchAll(re)){
    if(m.index>last)out.push({text:message.slice(last,m.index)});
    const kind=(Object.entries(m.groups??{}).find(([,v])=>v!==undefined)?.[0]) as Token['kind'];
    out.push({text:m[0],kind});
    last=m.index+m[0].length;
  }
  if(last<message.length)out.push({text:message.slice(last)});
  return out;
}

function time(iso:string){return new Date(iso).toLocaleTimeString(undefined,{hour12:false})}

/** The job's activity log: newest at the bottom, colored like a terminal, polled while the job is active. */
export function JobConsole({clusterId,job,active}:{clusterId:string;job:RebalanceJob;active:boolean}){
  const [events,setEvents]=useState<JobEvent[]>([]);
  const [open,setOpen]=useState(active);
  const [following,setFollowing]=useState(true);
  const box=useRef<HTMLDivElement>(null);
  const cursor=useRef(0);
  useEffect(()=>{setEvents([]);cursor.current=0;setOpen(active)},[job.id]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(()=>{if(active)setOpen(true)},[active]);
  useEffect(()=>{
    let stop=false;
    async function load(){
      try{
        for(;;){
          const r=await api<{events:JobEvent[];last:number}>(`/clusters/${encodeURIComponent(clusterId)}/rebalances/${encodeURIComponent(job.id)}/events?after=${cursor.current}`);
          if(stop)return;
          const fresh=r.data.events??[];
          if(fresh.length){cursor.current=r.data.last;setEvents(old=>[...old,...fresh].slice(-2000))}
          if(fresh.length<500)break;
        }
      }catch{/* the next poll retries */}
    }
    load();
    if(!active)return ()=>{stop=true};
    const id=setInterval(load,2000);
    return ()=>{stop=true;clearInterval(id)};
  },[clusterId,job.id,active,job.state]);
  useEffect(()=>{const el=box.current;if(el&&following)el.scrollTop=el.scrollHeight},[events,following,open]);
  const topics=useMemo(()=>(job.steps??[]).map(s=>s.topic).concat(job.topics??[]),[job.steps,job.topics]);
  const base=`/api/v1/clusters/${encodeURIComponent(clusterId)}/rebalances/${encodeURIComponent(job.id)}`;
  const running=job.state==='running';
  return <section className="panel job-console">
    <div className="job-console-head">
      <button type="button" className="job-console-toggle" aria-expanded={open} onClick={()=>setOpen(o=>!o)}><h2>Activity log</h2><span className="muted">{events.length} lines</span></button>
      <a className="button tint log" href={`${base}/events.txt`} download><FileText size={13}/>Download log</a>
      <a className="button tint backup" href={`${base}/backup.json`} download><ShieldCheck size={13}/>Download backup</a>
      <a className="button tint report" href={`${base}/report.csv`} download><Table2 size={13}/>Report CSV</a>
    </div>
    {open&&<div className="job-console-body" ref={box} role="log" aria-live="polite" onScroll={e=>{const el=e.currentTarget;setFollowing(el.scrollHeight-el.scrollTop-el.clientHeight<24)}}>
      {events.length===0&&<p className="job-console-empty">No activity yet. Lines appear here once the job is approved.</p>}
      {events.map((ev,i)=>{
        const glyph=[...ev.message][0];
        const tone=glyphs[glyph];
        const text=tone?ev.message.slice(glyph.length).trimStart():ev.message;
        const last=i===events.length-1&&running;
        return <div key={ev.seq} className={`job-line ${ev.level}`}>
          <span className="job-time">{time(ev.at)}</span>
          <span className={`job-glyph ${tone??'dim'}`}>{last?<BrailleSpinner/>:tone?glyph:'·'}</span>
          <span className="job-msg">{tokenize(text,topics).map((t,j)=>t.kind?<span key={j} className={`tok-${t.kind}`}>{t.text}</span>:t.text)}</span>
        </div>;
      })}
    </div>}
    {open&&!following&&<button type="button" className="button job-console-latest" onClick={()=>setFollowing(true)}><ArrowDownToLine size={13}/>Jump to latest</button>}
  </section>;
}
