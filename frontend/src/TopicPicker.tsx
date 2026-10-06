import {useEffect,useId,useState} from 'react';
import {useQuery} from '@tanstack/react-query';
import {Check,ChevronDown} from 'lucide-react';
import {api,type Topic} from './api';

const pageSize=50;

/** Searchable topic combobox: results come from the server so clusters with thousands of topics stay reachable.
 * With `selected`, it picks many topics: each choice toggles membership and the list stays open. */
export function TopicPicker({clusterId,value='',onChange,selected,label='Topic'}:{clusterId:string;value?:string;onChange:(topic:string)=>void;selected?:string[];label?:string}){
  const [text,setText]=useState(value);const [search,setSearch]=useState(value);const [open,setOpen]=useState(false);const [active,setActive]=useState(0);const id=useId();
  useEffect(()=>setText(value),[value]);
  useEffect(()=>{const t=setTimeout(()=>setSearch(text===value?'':text),200);return()=>clearTimeout(t)},[text,value]);
  const path=`/clusters/${clusterId}/topics?q=${encodeURIComponent(search)}&page=0&pageSize=${pageSize}`;
  const query=useQuery({queryKey:[path],queryFn:()=>api<Topic[]>(path),enabled:open,placeholderData:previous=>previous});
  const names=query.data?.data.map(x=>x.name)??[];const total=query.data?.meta?.total??names.length;
  useEffect(()=>setActive(0),[search]);
  const choose=(name:string)=>{if(selected){setText('');onChange(name);return}setText(name);setOpen(false);if(name!==value)onChange(name)};
  const close=()=>{setOpen(false);setText(value)};
  function key(e:React.KeyboardEvent<HTMLInputElement>){
    if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();if(!open){setOpen(true);return}if(names.length)setActive(i=>(i+(e.key==='ArrowDown'?1:names.length-1))%names.length)}
    else if(e.key==='Enter'&&open&&names[active]){e.preventDefault();choose(names[active])}
    else if(e.key==='Escape'&&open){e.preventDefault();close()}
  }
  return <div className="topic-picker">
    <input role="combobox" aria-label={label} aria-expanded={open} aria-controls={`${id}-list`} aria-autocomplete="list" aria-activedescendant={open&&names[active]?`${id}-${active}`:undefined} autoComplete="off" spellCheck={false} placeholder="Search topics…" value={text} onChange={e=>{setText(e.target.value);setOpen(true)}} onFocus={e=>{e.target.select();setOpen(true)}} onClick={()=>setOpen(true)} onBlur={close} onKeyDown={key}/>
    <ChevronDown size={14} aria-hidden/>
    {open&&<div className="topic-picker-popup">
      <ul role="listbox" id={`${id}-list`} aria-label="Topics">{names.map((name,i)=><li role="option" id={`${id}-${i}`} key={name} aria-selected={i===active} aria-checked={selected?selected.includes(name):undefined} className={(selected?selected.includes(name):name===value)?'current':undefined} onMouseDown={e=>e.preventDefault()} onMouseEnter={()=>setActive(i)} onClick={e=>{e.preventDefault();choose(name)}}>{selected&&<Check size={12} aria-hidden/>}{name}</li>)}</ul>
      {query.isLoading?<p>Searching…</p>:query.error?<p role="alert">{query.error.message}</p>:!names.length?<p>{search?`No topics match “${search}”.`:'No topics in this cluster.'}</p>:total>names.length&&<p>Showing {names.length} of {total} · keep typing to narrow</p>}
    </div>}
  </div>;
}
