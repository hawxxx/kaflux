import {Check,ChevronDown,Database} from 'lucide-react';
import {useEffect,useId,useRef,useState,type KeyboardEvent} from 'react';
import type {Cluster} from './api';
import './ClusterSelect.css';

/**
 * Cluster menu styled like the refresh interval picker: a trigger showing the current cluster and a
 * listbox that follows the ARIA listbox pattern (arrows, Home, End, Enter or Space to choose,
 * Escape or Tab to close).
 */
export function ClusterSelect({clusters,value,onChange}:{clusters:Cluster[];value:string;onChange:(id:string)=>void}){
  const [open,setOpen]=useState(false);
  const [active,setActive]=useState(0);
  const id=useId();
  const root=useRef<HTMLDivElement>(null);
  const trigger=useRef<HTMLButtonElement>(null);
  const list=useRef<HTMLUListElement>(null);
  const selectedIndex=Math.max(0,clusters.findIndex(x=>x.id===value));
  const current=clusters.find(x=>x.id===value);
  const show=(index=selectedIndex)=>{setActive(index);setOpen(true)};
  const close=(focusTrigger=true)=>{setOpen(false);if(focusTrigger)trigger.current?.focus()};
  const choose=(index:number)=>{const next=clusters[index];if(next&&next.id!==value)onChange(next.id);close()};
  useEffect(()=>{if(open)list.current?.focus()},[open]);
  useEffect(()=>{
    if(!open)return;
    const away=(e:PointerEvent)=>{if(!root.current?.contains(e.target as Node))setOpen(false)};
    document.addEventListener('pointerdown',away);
    return()=>document.removeEventListener('pointerdown',away);
  },[open]);
  const onTriggerKey=(e:KeyboardEvent)=>{
    if(['ArrowDown','ArrowUp','Enter',' '].includes(e.key)){e.preventDefault();show(e.key==='ArrowUp'?clusters.length-1:selectedIndex)}
  };
  const onListKey=(e:KeyboardEvent)=>{
    const last=clusters.length-1;
    const moves:Record<string,number>={ArrowDown:Math.min(last,active+1),ArrowUp:Math.max(0,active-1),Home:0,End:last};
    if(e.key in moves){e.preventDefault();setActive(moves[e.key])}
    else if(e.key==='Enter'||e.key===' '){e.preventDefault();choose(active)}
    else if(e.key==='Escape'){e.preventDefault();close()}
    else if(e.key==='Tab')close(false);
  };
  return <div className="cluster-picker" ref={root}>
    <button ref={trigger} type="button" className="cluster-trigger" aria-label={`Select cluster: ${current?.name??value}`} aria-haspopup="listbox" aria-expanded={open} aria-controls={open?`${id}-list`:undefined}
      onClick={()=>open?close(false):show()} onKeyDown={onTriggerKey}>
      <Database size={17} aria-hidden="true"/>
      <span className="cluster-trigger-text"><strong>{current?.name??value}</strong><small>{current?.environment??'Connecting'} <i/> {current?.mode??'Kafka'}</small></span>
      <ChevronDown size={14} aria-hidden="true" className="cluster-chevron"/>
    </button>
    {open&&<div className="refresh-menu cluster-menu">
      <div className="refresh-menu-title" id={`${id}-title`}>Cluster</div>
      <ul ref={list} id={`${id}-list`} role="listbox" tabIndex={-1} aria-labelledby={`${id}-title`} aria-activedescendant={`${id}-opt-${active}`} onKeyDown={onListKey}>
        {clusters.map((x,i)=><li key={x.id} id={`${id}-opt-${i}`} role="option" aria-selected={i===selectedIndex} data-active={i===active}
          onPointerEnter={()=>setActive(i)} onClick={()=>choose(i)}>
          <span>{x.name}</span><kbd>{x.environment}</kbd><Check size={13} aria-hidden="true" className="refresh-check"/>
        </li>)}
      </ul>
    </div>}
  </div>;
}
