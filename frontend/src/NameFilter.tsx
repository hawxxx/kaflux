import {useMemo} from 'react';
import {ops,opLabel,parseFilter,type Op,type Parsed} from './filter-query';
import './NameFilter.css';

function Pill({kind,children}:{kind:'or'|'not';children:React.ReactNode}){return <span className={`op-token op-${kind}`}>{children}</span>}

/** Chips that show how each typed word was read, with the operators colored. */
export function FilterChips({parsed,fallback}:{parsed:Parsed;fallback:Op}){
  const terms=parsed.chips.filter(c=>c.term&&!c.not);const kept=parsed.chips.filter(c=>c.term&&c.not);const bad=parsed.chips.filter(c=>c.problem);
  if(!parsed.chips.length)return null;
  return <div className="filter-chips" aria-label="How the filter is read">
    {terms.length>0&&<div className="chip-line"><span className="muted">Leaves out</span>{terms.map((c,i)=><span key={`${c.text}-${i}`} className="chip-group">{i>0&&<Pill kind="or">OR</Pill>}<span className="chip">{opLabel(c.term!.op)} <code>{c.term!.value}</code></span></span>)}</div>}
    {kept.length>0&&<div className="chip-line"><span className="muted">{terms.length?'Except':'Keeps'}</span>{kept.map((c,i)=><span key={`${c.text}-${i}`} className="chip-group"><Pill kind="not">NOT</Pill><span className="chip chip-not">{opLabel(c.term!.op)} <code>{c.term!.value}</code></span></span>)}</div>}
    {terms.length===0&&kept.length>0&&<p className="chip-hint muted">NOT only keeps topics. Add a word to leave out first, for example <code>tmp {kept[0].text.replace(/^(NOT|-)/,'')}</code>.</p>}
    {bad.length>0&&<div className="chip-line">{bad.map((c,i)=><span key={`${c.text}-${i}`} className="chip chip-problem" role="alert"><code>{c.text}</code> {c.problem}</span>)}</div>}
    <span className="muted chip-foot">Bare words use “{opLabel(fallback)}”.</span>
  </div>;
}

/** A match type menu and one keyword bar: words, OR, NOT and * override the menu. */
export function NameFilter({op,onOp,text,onText}:{op:Op;onOp:(o:Op)=>void;text:string;onText:(t:string)=>void}){
  const parsed=useMemo(()=>parseFilter(text,op),[text,op]);
  return <div className="name-filter">
    <div className="filter-heading"><span>Leave out topics whose name…</span><small className="muted">Separate words with a space or OR. NOT or - keeps a topic. * at the start or end sets the match.</small></div>
    <div className="filter-bar">
      <select aria-label="Match type" value={op} onChange={e=>onOp(e.target.value as Op)}>{ops.map(([v,l])=><option key={v} value={v}>{l}</option>)}</select>
      <input aria-label="Topic name filter" value={text} onChange={e=>onText(e.target.value)} maxLength={400} autoComplete="off" spellCheck={false} placeholder="tmp test NOT keep   or   *-dlq"/>
    </div>
    <FilterChips parsed={parsed} fallback={op}/>
  </div>;
}
