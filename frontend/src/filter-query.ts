// The keyword filter of the topic cleanup view. Words are separated by space or OR and any
// match leaves a topic out. NOT (or a leading -) keeps a topic: an except term always wins.
export type Op='starts'|'ends'|'contains'|'equals';
export const ops:[Op,string][]=[['starts','starts with'],['ends','ends with'],['contains','contains'],['equals','equals']];
export const opLabel=(op:Op)=>ops.find(x=>x[0]===op)![1];
export type Term={op:Op;value:string};
export type Chip={text:string;not:boolean;term?:Term;problem?:string};
export type Parsed={include:Term[];except:Term[];chips:Chip[]};
const MAX_TERMS=20;const MAX_VALUE=200;
const prefixes:Op[]=['starts','ends','contains','equals'];

function parseWord(raw:string,fallback:Op):{term?:Term;problem?:string}{
  let op:Op|undefined;let value=raw;
  const prefix=prefixes.find(p=>raw.startsWith(p+':'));
  if(prefix){op=prefix;value=raw.slice(prefix.length+1)}
  const lead=value.startsWith('*');const trail=value.length>1&&value.endsWith('*');
  const core=value.slice(lead?1:0,trail?-1:undefined);
  if(core.includes('*'))return {problem:'Use * only at the start or end of a word.'};
  if(!core)return {problem:'Add some text after the match type or the *.'};
  if(core.length>MAX_VALUE)return {problem:'Use at most 200 characters.'};
  if(!op){op=lead&&trail?'contains':lead?'ends':trail?'starts':fallback}
  else if(lead||trail)return {problem:`${op}: already says how to match, so drop the *.`};
  return {term:{op,value:core}};
}

/** Reads the typed filter. Words that cannot be used stay in chips with a problem and are not sent. */
export function parseFilter(text:string,fallback:Op):Parsed{
  const out:Parsed={include:[],except:[],chips:[]};
  let pendingNot=false;
  for(const token of text.split(/\s+/).filter(Boolean)){
    if(token==='OR')continue;
    if(token==='AND'){out.chips.push({text:token,not:false,problem:'AND is not needed: any word leaves a topic out. Use NOT to keep topics.'});continue}
    if(token==='NOT'){if(pendingNot)out.chips.push({text:token,not:true,problem:'NOT NOT is not supported.'});pendingNot=true;continue}
    let word=token;let not=pendingNot;pendingNot=false;
    if(word.length>1&&word.startsWith('-')){not=true;word=word.slice(1)}
    const {term,problem}=parseWord(word,fallback);
    if(!term){out.chips.push({text:token,not,problem});continue}
    const list=not?out.except:out.include;
    if(list.length>=MAX_TERMS){out.chips.push({text:token,not,problem:'At most 20 words to leave out and 20 to keep.'});continue}
    list.push(term);out.chips.push({text:token,not,term});
  }
  if(pendingNot)out.chips.push({text:'NOT',not:true,problem:'NOT needs a word after it.'});
  return out;
}

/** JSON for the server, or '' when there is nothing to send. */
export function filterParam(p:Parsed){return p.include.length||p.except.length?JSON.stringify({include:p.include,except:p.except}):''}
