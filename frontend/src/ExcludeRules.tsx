import {useRef,useState} from 'react';
import {Plus,X} from 'lucide-react';
import './ExcludeRules.css';

export type RuleOp='starts'|'ends'|'contains'|'equals'|'regex';
export type Rule={id:number;op:RuleOp;value:string;not:boolean;join:'and'|'or'};
export const ruleOps:[RuleOp,string][]=[['starts','starts with'],['ends','ends with'],['contains','contains'],['equals','equals'],['regex','matches regex']];
const MAX_RULES=20;const MAX_VALUE=200;

function regexOk(pattern:string){try{new RegExp(pattern);return true}catch{return false}}
/** An empty value is a draft, not an error. A set value must fit the server limits. */
export function ruleProblem(rule:Rule){
  if(rule.value.length>MAX_VALUE)return 'Use at most 200 characters.';
  if(rule.op==='regex'&&rule.value&&!regexOk(rule.value))return 'Not a valid regular expression.';
  return '';
}
const active=(rules:Rule[])=>rules.filter(r=>r.value.trim()!==''&&!ruleProblem(r));
/** JSON for the server, or '' when no rule is complete. Incomplete rules are left out. */
export function rulesParam(rules:Rule[]){
  const use=active(rules);
  return use.length?JSON.stringify(use.map(r=>({op:r.op,value:r.value.trim(),not:r.not,join:r.join}))):'';
}
const opLabel=(op:RuleOp)=>ruleOps.find(x=>x[0]===op)![1];

function Token({kind,children}:{kind:'not'|'or'|'and';children:React.ReactNode}){return <span className={`op-token op-${kind}`}>{children}</span>}
/** Plain-language reading of the rules, with the operators colored. AND binds tighter than OR. */
export function RulePreview({rules}:{rules:Rule[]}){
  const use=active(rules);
  if(!use.length)return null;
  return <p className="rule-preview" aria-label="Rule summary"><span className="muted">Leaves out topics whose name </span>{use.map((r,i)=><span key={r.id}>{i>0&&<> <Token kind={r.join}>{r.join.toUpperCase()}</Token> </>}{r.not&&<><Token kind="not">NOT</Token> </>}<span>{opLabel(r.op)}</span> <code>{r.value.trim()}</code></span>)}</p>;
}

/** Builds name filters from rows: a NOT toggle, a match type and a value, joined by AND or OR. */
export function ExcludeRules({rules,onChange}:{rules:Rule[];onChange:(r:Rule[])=>void}){
  const next=useRef(1);const [focusId,setFocusId]=useState<number|null>(null);
  const set=(id:number,patch:Partial<Rule>)=>onChange(rules.map(r=>r.id===id?{...r,...patch}:r));
  const add=()=>{const id=next.current++;setFocusId(id);onChange([...rules,{id,op:'starts',value:'',not:false,join:'or'}])};
  return <div className="exclude-rules">
    <div className="rule-heading"><span id="rules-heading">Leave out topics whose name…</span><small className="muted">AND binds tighter than OR. Rules with no value are ignored.</small></div>
    {rules.length===0&&<p className="muted rule-empty">No name rules. Every unused topic is listed.</p>}
    {rules.map((r,i)=>{const problem=ruleProblem(r);return <div key={r.id} className="rule-row">
      <div className="rule-join">{i>0&&<button type="button" className={`op-token op-${r.join}`} aria-label={`Rule ${i+1} joins the previous rule with ${r.join.toUpperCase()}. Switch to ${r.join==='or'?'AND':'OR'}`} onClick={()=>set(r.id,{join:r.join==='or'?'and':'or'})}>{r.join.toUpperCase()}</button>}</div>
      <div className="rule-body">
        <button type="button" className={`op-token op-not${r.not?' on':''}`} aria-pressed={r.not} aria-label={`Negate rule ${i+1}`} onClick={()=>set(r.id,{not:!r.not})}>NOT</button>
        <select aria-label={`Match type for rule ${i+1}`} value={r.op} onChange={e=>set(r.id,{op:e.target.value as RuleOp})}>{ruleOps.map(([v,l])=><option key={v} value={v}>{l}</option>)}</select>
        <input aria-label={`Value for rule ${i+1}`} aria-invalid={!!problem} value={r.value} maxLength={MAX_VALUE} autoComplete="off" autoFocus={r.id===focusId} placeholder={r.op==='regex'?'^(tmp|test)\\.':'tmp'} onChange={e=>set(r.id,{value:e.target.value})}/>
        <button type="button" className="rule-remove" aria-label={`Remove rule ${i+1}`} onClick={()=>onChange(rules.filter(x=>x.id!==r.id))}><X size={14} aria-hidden/></button>
      </div>
      {problem&&<small className="bad-text rule-problem" role="alert">{problem}</small>}
    </div>})}
    <div className="rule-actions"><button type="button" className="button" disabled={rules.length>=MAX_RULES} onClick={add}><Plus size={14} aria-hidden/> Add rule</button></div>
    <RulePreview rules={rules}/>
  </div>;
}
