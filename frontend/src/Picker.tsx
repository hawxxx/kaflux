import {useEffect,useId,useMemo,useRef,useState} from 'react';
import {Check,ChevronDown,Search} from 'lucide-react';
import './Picker.css';
export type PickerOption={value:string;label?:string};
/** Styled single-select listbox with type-to-filter search; same look as the days combobox. */
export function Picker({value,onChange,options,placeholder='Select…',label,searchable=options.length>7}:{value:string;onChange:(v:string)=>void;options:PickerOption[];placeholder?:string;label:string;searchable?:boolean}){
  const [open,setOpen]=useState(false);const [active,setActive]=useState(-1);const [query,setQuery]=useState('');
  const root=useRef<HTMLDivElement>(null);const list=useRef<HTMLUListElement>(null);const trigger=useRef<HTMLButtonElement>(null);const search=useRef<HTMLInputElement>(null);const id=useId();
  const selected=options.find(o=>o.value===value);
  const needle=query.trim().toLowerCase();
  const shown=useMemo(()=>needle?options.filter(o=>(o.label??o.value).toLowerCase().includes(needle)):options,[options,needle]);
  useEffect(()=>{if(!open)return;const close=(e:MouseEvent)=>{if(!root.current?.contains(e.target as Node))setOpen(false)};document.addEventListener('mousedown',close);return()=>document.removeEventListener('mousedown',close)},[open]);
  useEffect(()=>{if(open&&active>=0)list.current?.children[active]?.scrollIntoView?.({block:'nearest'})},[open,active]);
  useEffect(()=>{if(open&&searchable)search.current?.focus()},[open,searchable]);
  const show=()=>{setQuery('');setActive(Math.max(0,options.findIndex(o=>o.value===value)));setOpen(true)};
  const close=(refocus=true)=>{setOpen(false);if(refocus)trigger.current?.focus()};
  const pick=(v:string)=>{onChange(v);close()};
  function key(e:React.KeyboardEvent){
    const typing=e.target===search.current;
    if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();if(!open){show();return}if(shown.length)setActive(i=>(i+(e.key==='ArrowDown'?1:-1)+shown.length)%shown.length)}
    else if(e.key==='Enter'||(e.key===' '&&!typing)){e.preventDefault();if(!open)show();else if(active>=0&&shown[active])pick(shown[active].value)}
    else if(e.key==='Escape'&&open){e.preventDefault();e.stopPropagation();close()}
    else if(e.key==='Home'&&open&&!typing){e.preventDefault();setActive(0)}
    else if(e.key==='End'&&open&&!typing){e.preventDefault();setActive(shown.length-1)}
    else if(e.key==='Tab'&&open)setOpen(false)}
  const activeId=open&&active>=0&&shown[active]?`${id}-${active}`:undefined;
  return <div className="picker" ref={root}>
    <button ref={trigger} type="button" className="picker-trigger" role="combobox" aria-haspopup="listbox" aria-expanded={open} aria-controls={`${id}-list`} aria-label={label} aria-activedescendant={searchable?undefined:activeId} onClick={()=>open?close(false):show()} onKeyDown={key}>
      <span className={selected?undefined:'picker-placeholder'}>{selected?(selected.label??selected.value):placeholder}</span><ChevronDown size={14} aria-hidden/>
    </button>
    {open&&<div className="picker-pop" onKeyDown={searchable?key:undefined}>
      {searchable&&<div className="picker-search"><Search size={13} aria-hidden/><input ref={search} value={query} onChange={e=>{setQuery(e.target.value);setActive(0)}} placeholder="Search…" aria-label={`Search ${label}`} aria-controls={`${id}-list`} aria-activedescendant={activeId} autoComplete="off" spellCheck={false}/></div>}
      <ul id={`${id}-list`} ref={list} role="listbox" aria-label={label}>
        {shown.map((o,i)=><li key={o.value} id={`${id}-${i}`} role="option" aria-selected={o.value===value} className={i===active?'active':undefined} onMouseDown={e=>e.preventDefault()} onMouseEnter={()=>setActive(i)} onClick={()=>pick(o.value)}><span>{o.label??o.value}</span>{o.value===value&&<Check size={13} aria-hidden/>}</li>)}
        {shown.length===0&&<li className="picker-empty" role="presentation">No matches</li>}
      </ul>
    </div>}
  </div>;
}
