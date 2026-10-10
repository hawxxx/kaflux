import {useLayoutEffect,useRef,useState} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
import {ArrowDownToLine,ChevronRight,Copy,Pause,Play,RefreshCw,ScanSearch,Search,SendHorizontal,X} from 'lucide-react';
import {api,bytes,type Message} from './api';
import {MessageValue} from './MessageValue';
import {TopicPicker} from './TopicPicker';
import {useTimeZone} from './TimeZone';
import {useEssentialInterval} from './RefreshControl';
import {Empty,ErrorBox} from './Feedback';
import {LoadingPanel,UpdatingBar} from './LoadingPanel';
import {chronological,filterMessages,mergeMessages,type SearchScope} from './message-filters';
import {ProduceDialog,draftFrom,emptyDraft,type Draft} from './ProduceDialog';
import type {TopicDetail} from './TopicOverview';
import './Messages.css';

type Start='latest'|'earliest'|'offset'|'time';
const starts:[Start,string][]=[['latest','Latest'],['earliest','Earliest'],['offset','Offset'],['time','Time']];
const recordId=(m:Message)=>`${m.partition}-${m.offset}`;
type SearchBody={topic:string;partitions?:number[];from?:{timestamp?:string;offsets?:Record<string,number>};match:{in:string;text:string}};
type SearchResult={matches:Message[];scanned:{records:number;bytes:number};resume:Record<string,number>|null;done:boolean;stoppedBy:string};
type Scan=SearchResult&{text:string;body:SearchBody};
const stopReasons:Record<string,string>={end:'Scanned to the end of the range',matches:'Stopped at the match limit',records:'Stopped after the record limit',bytes:'Stopped after the byte limit',time:'Stopped at the 30 s time limit'};
const count=new Intl.NumberFormat();
const byteLength=(base64?:string)=>base64==null?undefined:Math.floor(base64.length*3/4)-(base64.endsWith('==')?2:base64.endsWith('=')?1:0);
// One-line preview of a payload; schema-decoded content wins over the raw text.
function preview(value:unknown,decoded?:unknown){
  const source=decoded!==undefined?decoded:value;
  if(source==null)return '';
  if(typeof source!=='string')return JSON.stringify(source)??'';
  try{return JSON.stringify(JSON.parse(source))}catch{return source}
}

// Live tail keeps the newest record in view while the reader sits at the bottom of the list.
// Scrolled up, arrivals are counted instead so the reader's place is never yanked away.
function useTailFollow(keys:string[],active:boolean){const ref=useRef<HTMLDivElement>(null);const pinned=useRef(true);const seen=useRef(new Set<string>());const [unseen,setUnseen]=useState(0);const signature=keys.join('|');
  const toBottom=(behavior:ScrollBehavior)=>{const el=ref.current;if(el)el.scrollTo({top:el.scrollHeight,behavior});pinned.current=true;setUnseen(0)};
  const onScroll=()=>{const el=ref.current;if(!el)return;pinned.current=el.scrollHeight-el.scrollTop-el.clientHeight<24;if(pinned.current)setUnseen(0)};
  useLayoutEffect(()=>{if(active)toBottom('auto');else setUnseen(0)},[active]);
  useLayoutEffect(()=>{const first=seen.current.size===0;const fresh=keys.filter(k=>!seen.current.has(k)).length;seen.current=new Set(keys);if(!active||first||!fresh)return;if(pinned.current)toBottom('auto');else setUnseen(n=>n+fresh)},[signature,active]);
  return {ref,onScroll,unseen,jump:()=>toBottom('smooth')}}

export function Messages({clusterId,initialTopic,canProduce}:{clusterId:string;initialTopic?:string;canProduce:boolean}){
  const tz=useTimeZone();
  const qc=useQueryClient();
  const [topic,setTopic]=useState(initialTopic??'');
  const [partition,setPartition]=useState('all');
  const [start,setStart]=useState<Start>('latest');
  const [offset,setOffset]=useState('0');
  const [timestamp,setTimestamp]=useState('');
  const [limit,setLimit]=useState('50');
  const [search,setSearch]=useState('');
  const [scope,setScope]=useState<SearchScope>('all');
  const [registry,setRegistry]=useState('');
  const [decoderTarget,setDecoderTarget]=useState('value');
  const [tail,setTail]=useState(false);
  const [expanded,setExpanded]=useState<Set<string>>(new Set());
  const [producing,setProducing]=useState(false);
  const [draft,setDraft]=useState<Draft>(emptyDraft);
  const [notice,setNotice]=useState('');
  const [scan,setScan]=useState<Scan|null>(null);
  const [scanning,setScanning]=useState<AbortController|null>(null);
  const [scanError,setScanError]=useState<Error|null>(null);
  const registries=useQuery({queryKey:[`/clusters/${clusterId}/schemas`],queryFn:()=>api<{id:string}[]>(`/clusters/${clusterId}/schemas`)});
  const detailPath=`/clusters/${clusterId}/topics/${encodeURIComponent(topic)}`;
  const detail=useQuery({queryKey:[detailPath],queryFn:()=>api<TopicDetail>(detailPath),enabled:!!topic});
  const all=partition==='all';
  const partitions=[...detail.data?.data.partitions??[]].sort((a,b)=>a.id-b.id);
  const params=new URLSearchParams({topic,limit,partition,decoderTarget});
  if(registry){params.set('decoder',registry.startsWith('protobuf:')?'protobuf':'avro');params.set('registry',registry.replace(/^protobuf:/,''))}
  if(start==='offset')params.set('offset',offset||'0');
  if(start==='time'){const at=tz.parse(timestamp);if(Number.isFinite(at.getTime()))params.set('timestamp',at.toISOString())}
  const signature=`${start}:${params}`;
  const tailState=useRef<{signature:string;cursors:Record<number,number>;records:Message[]}>({signature:'',cursors:{},records:[]});
  const tailEvery=useEssentialInterval(3_000,tail);
  const query=useQuery({queryKey:['message-read',clusterId,signature,tail],enabled:!!topic&&(start!=='time'||params.has('timestamp')),refetchInterval:tailEvery,
    placeholderData:(previous,previousQuery)=>previousQuery?.queryKey[2]===signature?previous:undefined,
    queryFn:async()=>{
      if(tailState.current.signature!==signature)tailState.current={signature,cursors:{},records:[]};
      const state=tailState.current;
      const single=all?undefined:Number(partition);
      // While tailing, cursors replace partition ends, so cached topic details are enough.
      const resumed=tail&&(single!==undefined?state.cursors[single]!==undefined:Object.keys(state.cursors).length>0);
      const wantRange=(start==='latest'||start==='earliest')&&!resumed;
      // Ends are resolved per fetch so "Latest" always means the newest records at the time of reading.
      const ranges=all||wantRange?(await qc.fetchQuery({queryKey:[detailPath],queryFn:()=>api<TopicDetail>(detailPath),staleTime:wantRange&&start==='latest'?0:15_000})).data.partitions:[];
      const targets=all?ranges.map(p=>p.id).sort((a,b)=>a-b):[single!];
      const offsetFor=(id:number)=>{
        const cursor=state.cursors[id];
        if(tail&&cursor!==undefined)return cursor;
        if(start==='latest'||start==='earliest'){
          const p=ranges.find(x=>x.id===id);
          const first=p?.startOffset??0;
          return start==='earliest'?first:Math.max(first,(p?.endOffset??0)-Number(limit));
        }
        return start==='offset'?Number(offset||'0'):0;
      };
      const requested=Object.fromEntries(targets.map(id=>[id,offsetFor(id)]));
      const read=new URLSearchParams(params);
      read.delete('partition');read.delete('offset');
      if(all)read.set('offsets',targets.map(id=>`${id}:${requested[id]}`).join(','));
      else{read.set('partition',String(single));read.set('offset',String(requested[single!]))}
      if(tail&&targets.every(id=>state.cursors[id]!==undefined))read.delete('timestamp');
      const byTime=read.has('timestamp');
      const incoming=(await api<Message[]>(`/clusters/${clusterId}/messages?${read}`)).data??[];
      // Resume each partition after what it returned. A partition that returned nothing resumes where it
      // was asked to start, since the server may have stopped early; only a timestamp read, whose start
      // the server chose, falls back to the end offset known before the request.
      for(const id of targets){
        const mine=incoming.filter(m=>m.partition===id);
        const next=mine.length?Math.max(...mine.map(m=>m.offset))+1:byTime?ranges.find(x=>x.id===id)?.endOffset??undefined:requested[id];
        if(next!==undefined)state.cursors[id]=Math.max(state.cursors[id]??0,next);
      }
      const keep=Number(limit);
      const bounded=all?(start==='latest'?mergeMessages([],incoming,keep,true):[...incoming].sort(chronological).slice(0,keep)):incoming;
      state.records=tail?mergeMessages(state.records,incoming,keep,all):bounded;
      return {data:tail?state.records:bounded};
    }});
  const returned=query.data?.data??[];
  const records=scan?scan.matches:filterMessages(returned,{text:search,scope});
  // Scanning reads the topic on the server and returns only matches; "Continue" resumes where a budget stopped it.
  async function runScan(previous?:Scan){
    const ctrl=new AbortController();setScanning(ctrl);setScanError(null);setTail(false);
    const from=start==='time'&&params.has('timestamp')?{timestamp:params.get('timestamp')!}:start==='offset'&&!all?{offsets:{[partition]:Number(offset||'0')}}:undefined;
    const body:SearchBody=previous?{...previous.body,partitions:undefined,from:{offsets:previous.resume??{}}}:{topic,partitions:all||from?.offsets?undefined:[Number(partition)],from,match:{in:scope==='all'?'any':scope,text:search.trim()}};
    try{
      const r=(await api<SearchResult>(`/clusters/${clusterId}/messages/search`,{method:'POST',body:JSON.stringify(body),signal:ctrl.signal})).data;
      setScan({...r,text:previous?.text??search.trim(),body:previous?.body??body,matches:[...previous?.matches??[],...r.matches],
        scanned:{records:(previous?.scanned.records??0)+r.scanned.records,bytes:(previous?.scanned.bytes??0)+r.scanned.bytes}});
    }catch(e){if(!ctrl.signal.aborted)setScanError(e as Error)}
    finally{setScanning(null)}
  }
  const resetScan=()=>{scanning?.abort();setScan(null);setScanError(null)};
  const follow=useTailFollow(records.map(recordId),tail);
  const allOpen=records.length>0&&records.every(m=>expanded.has(recordId(m)));
  const toggle=(id:string)=>setExpanded(s=>{const next=new Set(s);if(!next.delete(id))next.add(id);return next});
  const fetching=query.isFetching&&!tail;
  const range=!all&&returned.length?`offsets ${Math.min(...returned.map(m=>m.offset))}–${Math.max(...returned.map(m=>m.offset))}`:'';
  // A blank producer targets the partition being read, so a produced record shows up in this view.
  const openProducer=(next?:Draft)=>{setDraft(next??(d=>({...d,partition:all?'auto':partition})));setProducing(true)};
  return <>
    <section className="panel message-controls">
      <div className="message-read">
        {initialTopic===undefined&&<label className="message-topic">Topic<TopicPicker clusterId={clusterId} value={topic} onChange={t=>{setTopic(t);setPartition('all');setExpanded(new Set());resetScan()}}/></label>}
        <label>Partition
          <select value={partition} onChange={e=>{setPartition(e.target.value);if(e.target.value==='all'&&start==='offset')setStart('latest')}} disabled={!topic}>
            <option value="all">All partitions</option>
            {partitions.map(p=><option key={p.id} value={p.id}>Partition {p.id}</option>)}
          </select>
        </label>
        <div className="message-start">
          <span id="message-start-label">Start from</span>
          <div className="segmented" role="group" aria-labelledby="message-start-label">{starts.filter(([id])=>!(all&&id==='offset')).map(([id,label])=><button key={id} type="button" className={start===id?'selected':''} aria-pressed={start===id} onClick={()=>setStart(id)}>{label}</button>)}</div>
        </div>
        {start==='offset'&&<label>Starting offset<input type="number" min="0" value={offset} onChange={e=>setOffset(e.target.value)}/></label>}
        {start==='time'&&<label>Timestamp · {tz.label}<input type="datetime-local" value={timestamp} onChange={e=>setTimestamp(e.target.value)}/></label>}
        <label>Record limit<select value={limit} onChange={e=>setLimit(e.target.value)}>{[25,50,100].map(x=><option key={x}>{x}</option>)}</select></label>
      </div>
      <div className="message-refine">
        <div className="message-search">
          <Search size={14} aria-hidden="true"/>
          <input type="search" aria-label="Search returned records" placeholder="Search returned records…" value={search} onChange={e=>setSearch(e.target.value)}/>
          <select aria-label="Search in" value={scope} onChange={e=>setScope(e.target.value as SearchScope)}>
            <option value="all">Everywhere</option><option value="key">Key</option><option value="value">Value</option><option value="headers">Headers</option>
          </select>
        </div>
        <label>Decoder
          <select aria-label="Value decoder" value={registry} onChange={e=>setRegistry(e.target.value)}>
            <option value="">Raw / JSON</option>
            {registries.data?.data.flatMap(x=>[<option key={x.id} value={x.id}>Avro · {x.id}</option>,<option key={`protobuf:${x.id}`} value={`protobuf:${x.id}`}>Protobuf · {x.id}</option>])}
          </select>
        </label>
        {registry&&<label>Decode<select aria-label="Decode field" value={decoderTarget} onChange={e=>setDecoderTarget(e.target.value)}><option value="value">Value</option><option value="key">Key</option><option value="both">Key and value</option></select></label>}
        <div className="message-buttons">
          <button className="button" title="Scan the topic on the server for records matching the search" disabled={!topic||!search.trim()||!!scanning} onClick={()=>runScan()}><ScanSearch size={14}/>Scan topic</button>
          <button className={`button ${tail?'':'primary'}`} disabled={!topic||!!scan} onClick={()=>setTail(!tail)}>{tail?<Pause size={14}/>:<Play size={14}/>}{tail?'Pause live tail':'Start live tail'}</button>
          <button className="button" disabled={!topic||fetching} aria-busy={fetching} onClick={()=>query.refetch()}><RefreshCw size={14} className={fetching?'spin':undefined}/>{fetching?'Fetching…':'Fetch'}</button>
          <button className="button" title={canProduce?'Produce a record':'Your role cannot produce messages'} disabled={!canProduce||!topic} onClick={()=>openProducer()}><SendHorizontal size={14}/>Produce</button>
        </div>
      </div>
    </section>
    <ErrorBox error={scanError??query.error}/>
    {!topic?<Empty text="Choose a topic to begin inspecting messages."/>:<section className={`panel message-results${tail?' live':''}`}>
      <div className="message-results-heading">
        {scan?<div>
          <strong>{scan.matches.length} {scan.matches.length===1?'match':'matches'} for “{scan.text}”</strong>
          <span>Scanned {count.format(scan.scanned.records)} records · {bytes(scan.scanned.bytes)}</span>
          <span className="message-tail-state"><i className={scan.done?'good-dot':'paused-dot'}/>{stopReasons[scan.stoppedBy]??scan.stoppedBy}</span>
        </div>:<div>
          <strong>{search?`${records.length} of ${returned.length}`:returned.length} records</strong>
          <span>{all?'All partitions':`Partition ${partition}`}{range&&` · ${range}`}</span>
          <span className="message-tail-state"><i className={tail?'good-dot':'paused-dot'}/>{tail?'Live · polling every 3s':'Bounded read'}</span>
        </div>}
        <div className="message-results-actions">
          {scan&&!scan.done&&<button className="link-button" disabled={!!scanning} onClick={()=>runScan(scan)}><ScanSearch size={13}/>Continue scanning</button>}
          {scan&&<button className="link-button" onClick={resetScan}>Back to records</button>}
          <button className="link-button" disabled={!records.length} onClick={()=>setExpanded(allOpen?new Set():new Set(records.map(recordId)))}>{allOpen?'Collapse all':'Expand all'}</button>
        </div>
      </div>
      <UpdatingBar active={fetching} label="Fetching records"/>
      <div className="message-columns" aria-hidden="true"><span/><span>{all||scan?'Part · Offset':'Offset'}</span><span>Timestamp</span><span>Key</span><span>Value</span><span>Size</span></div>
      <div className="message-list" ref={follow.ref} onScroll={follow.onScroll}>
        {scanning?<div className="scan-progress"><LoadingPanel label={`Scanning ${topic} for “${scan?.text??search.trim()}”`} slowAfterSeconds={10}/><button className="button" onClick={()=>scanning.abort()}><X size={14}/>Cancel scan</button></div>
        :query.isLoading&&!scan?<LoadingPanel label={all?'Reading every partition':`Reading partition ${partition}`}/>
        :!records.length?<Empty text={scan?`No records match “${scan.text}” in the scanned range.`:returned.length?'No returned records match this search.':'No records returned within the read limits.'}/>:records.map(m=>{
          const id=recordId(m),open=expanded.has(id),key=preview(m.key,m.decodedKey),size=byteLength(m.valueBase64);
          return <div className={`record${open?' open':''}`} key={id}>
            <button className="record-summary" aria-expanded={open} aria-controls={`record-${id}`} onClick={()=>toggle(id)}>
              <ChevronRight size={14} className="record-chevron" aria-hidden="true"/>
              <span className="record-offset" title={`Partition ${m.partition}, offset ${m.offset}`}>{(all||scan)&&<>p{m.partition}<b>·</b></>}{m.offset}</span>
              <span className="record-time" title={tz.dateTime(m.timestamp)}>{tz.time(m.timestamp)}</span>
              <span className={`record-key${key?'':' none'}`}>{key||'no key'}</span>
              <span className="record-value">{preview(m.value,m.decodedValue)||<em>empty</em>}</span>
              <span className="record-meta">{m.headers?.length?<span className="record-headers" title={`${m.headers.length} headers`}>H{m.headers.length}</span>:null}{size!==undefined&&bytes(size)}{m.truncated&&<span className="record-truncated" title="Preview truncated">…</span>}</span>
            </button>
            {open&&<RecordDetail id={`record-${id}`} record={m} canProduce={canProduce} onReproduce={()=>openProducer(draftFrom(m))}/>}
          </div>;
        })}
        {follow.unseen>0&&<button className="tail-pill" onClick={follow.jump}><ArrowDownToLine size={12}/>{follow.unseen} new</button>}
      </div>
    </section>}
    <ProduceDialog open={producing} onOpenChange={setProducing} clusterId={clusterId} topic={topic} partitions={partitions.length||undefined} draft={draft} onDraft={setDraft}
      onProduced={m=>{setNotice(`Record produced to partition ${m.partition} at offset ${m.offset}.`);query.refetch()}}/>
    {notice&&<div role="status" className="toast" onClick={()=>setNotice('')}>{notice}</div>}
  </>;
}

type Section='Value'|'Key'|'Headers';
function RecordDetail({id,record:m,canProduce,onReproduce}:{id:string;record:Message;canProduce:boolean;onReproduce:()=>void}){
  const tz=useTimeZone();
  const [section,setSection]=useState<Section>('Value');
  const [copied,setCopied]=useState(false);
  const headers=m.headers??[];
  async function copy(){
    await navigator.clipboard.writeText(JSON.stringify({partition:m.partition,offset:m.offset,timestamp:m.timestamp,key:m.decodedKey??m.key,value:m.decodedValue??m.value,headers},null,2));
    setCopied(true);setTimeout(()=>setCopied(false),1600);
  }
  return <div className="record-detail" id={id}>
    <div className="record-detail-bar">
      <div className="segmented" role="tablist" aria-label="Record part">{(['Value','Key','Headers'] as Section[]).map(x=><button key={x} role="tab" aria-selected={section===x} className={section===x?'selected':''} onClick={()=>setSection(x)}>{x}{x==='Headers'&&` · ${headers.length}`}</button>)}</div>
      <span className="record-stamp">P{m.partition} · offset {m.offset} · {tz.dateTime(m.timestamp)}</span>
      <div className="record-actions">
        <button className="link-button" onClick={copy}><Copy size={13}/>{copied?'Copied':'Copy record'}</button>
        {canProduce&&<button className="link-button" disabled={m.truncated} title={m.truncated?'Only a truncated preview was returned; the original payload cannot be reproduced.':'Open the producer with this key, value and headers'} onClick={onReproduce}><SendHorizontal size={13}/>Produce copy</button>}
      </div>
    </div>
    {m.truncated&&section!=='Headers'&&<p className="truncated-notice" role="status">Preview truncated to the server byte limit. Copy and download contain the returned preview.</p>}
    {section==='Value'?<MessageValue key="value" value={m.value} bytesBase64={m.valueBase64} decodedValue={m.decodedValue} schemaId={m.schemaId} decodeError={m.decodeError} decodedFormat={m.decodedFormat}/>
    :section==='Key'?<MessageValue key="key" value={m.key} bytesBase64={m.keyBase64} decodedValue={m.decodedKey} decodedFormat={m.keyDecodedFormat} schemaId={m.keySchemaId} decodeError={m.keyDecodeError}/>
    :headers.length?<table className="header-table"><thead><tr><th>Header</th><th>Value</th></tr></thead><tbody>{headers.map((h,i)=><tr key={i}><td>{h.key}</td><td>{h.value}</td></tr>)}</tbody></table>
    :<p className="muted record-empty">This record has no headers.</p>}
  </div>;
}
