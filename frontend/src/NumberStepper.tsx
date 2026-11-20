import type {KeyboardEvent} from 'react';
import {Minus,Plus} from 'lucide-react';
import './NumberStepper.css';

/** Moves `value` by `delta` and keeps the result inside [min, max]. A blank or unparsable value starts from `min`. */
export function clampStep(value:string,delta:number,min:number,max:number,step:number){
  const current=Number.parseInt(value,10);
  const next=Number.isFinite(current)?current+delta*step:min;
  return Math.min(max,Math.max(min,next));
}

type Props={value:string;onChange:(value:string)=>void;min:number;max:number;step?:number;label:string;id?:string;disabled?:boolean;invalid?:boolean;describedBy?:string;valueText?:(value:number)=>string};

/**
 * Whole-number field with explicit − and + buttons. It is a text input with the spinbutton role, so no
 * browser spinner appears; arrow, Page Up/Down, Home and End keys step it, and typed values are kept
 * as entered so the surrounding form can report out-of-range input instead of silently changing it.
 */
export function NumberStepper({value,onChange,min,max,step=1,label,id,disabled,invalid,describedBy,valueText}:Props){
  const parsed=/^-?\d+$/.test(value)?Number(value):NaN;
  const inRange=Number.isFinite(parsed)&&parsed>=min&&parsed<=max;
  const set=(delta:number)=>onChange(String(clampStep(value,delta,min,max,step)));
  function key(event:KeyboardEvent<HTMLInputElement>){
    const moves:Record<string,()=>void>={ArrowUp:()=>set(1),ArrowDown:()=>set(-1),PageUp:()=>set(10),PageDown:()=>set(-10),Home:()=>onChange(String(min)),End:()=>onChange(String(max))};
    const move=moves[event.key];
    if(!move)return;
    event.preventDefault();
    move();
  }
  const pattern=min<0?/^-?\d*$/:/^\d*$/;
  return <span className={`number-stepper${invalid?' invalid':''}`}>
    <button type="button" className="stepper-button" aria-label={`Decrease ${label}`} tabIndex={-1} disabled={disabled||(Number.isFinite(parsed)&&parsed<=min)} onClick={()=>set(-1)}><Minus size={13} aria-hidden="true"/></button>
    <input id={id} type="text" role="spinbutton" inputMode="numeric" autoComplete="off" value={value} disabled={disabled}
      aria-label={id?undefined:label} aria-valuemin={min} aria-valuemax={max} aria-valuenow={inRange?parsed:undefined} aria-valuetext={inRange&&valueText?valueText(parsed):undefined}
      aria-invalid={invalid||undefined} aria-describedby={describedBy}
      onKeyDown={key} onChange={event=>{if(pattern.test(event.target.value))onChange(event.target.value)}}/>
    <button type="button" className="stepper-button" aria-label={`Increase ${label}`} tabIndex={-1} disabled={disabled||(Number.isFinite(parsed)&&parsed>=max)} onClick={()=>set(1)}><Plus size={13} aria-hidden="true"/></button>
  </span>;
}
