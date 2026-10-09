import {useEffect,useState} from 'react';

const frames='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏';

function reducedMotion(){try{return window.matchMedia?.('(prefers-reduced-motion: reduce)').matches??false}catch{return false}}

/** A terminal-style braille spinner; it stays on its first frame when motion is reduced. */
export function BrailleSpinner({className=''}:{className?:string}){
  const [frame,setFrame]=useState(0);
  useEffect(()=>{if(reducedMotion())return;const id=setInterval(()=>setFrame(f=>(f+1)%frames.length),80);return ()=>clearInterval(id)},[]);
  return <span className={`braille-spinner ${className}`} aria-hidden="true">{frames[frame]}</span>;
}
