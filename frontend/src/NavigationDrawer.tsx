import {useEffect,useRef,useState,type ReactNode} from 'react';
import {X} from 'lucide-react';

export function NavigationDrawer({open,onOpenChange,children}:{open:boolean;onOpenChange:(open:boolean)=>void;children:ReactNode}){
  const [narrow,setNarrow]=useState(()=>window.matchMedia('(max-width: 700px)').matches);
  const sidebar=useRef<HTMLElement>(null);
  const active=narrow&&open;

  useEffect(()=>{
    const media=window.matchMedia('(max-width: 700px)');
    const change=()=>{setNarrow(media.matches);if(!media.matches)onOpenChange(false)};
    media.addEventListener('change',change);
    return()=>media.removeEventListener('change',change);
  },[onOpenChange]);

  useEffect(()=>{
    if(!active)return;
    const element=sidebar.current!;
    const shell=document.querySelector<HTMLElement>('.main-shell');
    const previousFocus=document.activeElement instanceof HTMLElement?document.activeElement:null;
    const previousOverflow=document.body.style.overflow;
    const shellWasInert=shell?.inert??false;
    if(shell)shell.inert=true;
    document.body.style.overflow='hidden';
    const focusable=()=>Array.from(element.querySelectorAll<HTMLElement>('a[href],button:not(:disabled),input:not(:disabled),select:not(:disabled),[tabindex="0"]')).filter(e=>e.getClientRects().length>0);
    focusable()[0]?.focus();
    const key=(event:KeyboardEvent)=>{
      if(event.key==='Escape'){event.preventDefault();onOpenChange(false)}
      if(event.key!=='Tab')return;
      const targets=focusable(),first=targets[0],last=targets[targets.length-1];
      if(!first){event.preventDefault();element.focus();return}
      if(event.shiftKey&&(document.activeElement===first||!element.contains(document.activeElement))){event.preventDefault();last.focus()}
      else if(!event.shiftKey&&(document.activeElement===last||!element.contains(document.activeElement))){event.preventDefault();first.focus()}
    };
    document.addEventListener('keydown',key);
    return()=>{
      document.removeEventListener('keydown',key);
      if(shell)shell.inert=shellWasInert;
      document.body.style.overflow=previousOverflow;
      if(previousFocus?.isConnected)previousFocus.focus();
    };
  },[active,onOpenChange]);

  return <>
    {active&&<div className="navigation-backdrop" onClick={()=>onOpenChange(false)} aria-hidden="true"/>}
    <aside ref={sidebar} id="workspace-navigation" className={`sidebar ${active?'open':''}`} inert={narrow&&!active} role={active?'dialog':undefined} aria-modal={active?true:undefined} aria-label="Workspace navigation" tabIndex={-1}>
      {active&&<button className="navigation-close icon-button" onClick={()=>onOpenChange(false)} aria-label="Close navigation"><X size={20}/></button>}
      {children}
    </aside>
  </>;
}
