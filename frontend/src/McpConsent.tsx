import {useEffect,useRef,useState} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
import {ArrowRight,Check,CircleAlert,ShieldCheck,X} from 'lucide-react';
import '@fontsource-variable/geist';
import '@fontsource/newsreader/400-italic.css';
import {api,ApiError,setCsrf,type Session} from './api';
import {Logo} from './Logo';
import {Login} from './Login';
import {LoginBackdrop} from './LoginBackdrop';
import {SCOPES} from './McpAccess';
import './login.css';
import './McpConsent.css';

/** A pending authorization request, created by the server when an MCP client starts the sign-in flow. */
export type ConsentRequest={client:{name:string;host:string};scopes:string[];clusters:{id:string;name:string}[];ownerScopes:string[];accessTokenMinutes:number;refreshTokenDays:number};
type Outcome={redirect:string};

/** The first two letters of the client name, for the avatar tile. */
const REDIRECT_DELAY_MS=1800;
function initials(name:string){return (name.trim().slice(0,2)||'··').toLowerCase()}

/**
 * Consent screen for MCP clients that sign in with OAuth. It shares the sign-in screen's look, shows who is asking,
 * and lets the user grant less than the client requested. The server re-checks every choice.
 */
export function McpConsent({requestId}:{requestId:string}){
  const client=useQueryClient();
  const session=useQuery({queryKey:['/auth/session'],queryFn:()=>api<Session>('/auth/session'),retry:false});
  const request=useQuery({queryKey:['mcp-oauth-request',requestId],queryFn:()=>api<ConsentRequest>(`/mcp/oauth/requests/${encodeURIComponent(requestId)}`),enabled:!!session.data,retry:false});
  // Writes carry the session's CSRF token; this page loads the session itself, so it must hand the token to the API client.
  if(session.data)setCsrf(session.data.data.csrfToken);
  const [scopes,setScopes]=useState<string[]|null>(null);const [allowed,setAllowed]=useState<string[]>([]);
  const [busy,setBusy]=useState<''|'allow'|'deny'>('');const [error,setError]=useState('');
  // Once decided, say so here and hand the browser back to the client a moment later.
  const [done,setDone]=useState<{kind:'allow'|'deny';redirect:string}|null>(null);
  useEffect(()=>{if(!done)return;const t=window.setTimeout(()=>window.location.assign(done.redirect),REDIRECT_DELAY_MS);return()=>window.clearTimeout(t)},[done]);
  const card=useRef<HTMLDivElement>(null);
  const data=request.data?.data;
  const blocked=request.error instanceof ApiError&&request.error.status===403;
  const chosen=scopes??data?.scopes.filter(s=>data.ownerScopes.includes(s))??[];
  const toggle=(list:string[],v:string)=>list.includes(v)?list.filter(x=>x!==v):[...list,v];

  if(session.isError)return <Login heading={{plain:'Connect an AI client,',serif:'on your terms.'}} lead="Sign in to review what this client is asking for." onSuccess={()=>{client.invalidateQueries({queryKey:['/auth/session']})}}/>;
  async function decide(kind:'allow'|'deny'){
    setBusy(kind);setError('');
    try{
      const body=kind==='allow'?{scopes:SCOPES.map(s=>s.id).filter(s=>chosen.includes(s)),clusters:allowed}:{};
      const result=await api<Outcome>(`/mcp/oauth/requests/${encodeURIComponent(requestId)}/${kind==='allow'?'approve':'deny'}`,{method:'POST',body:JSON.stringify(body)});
      setDone({kind,redirect:result.data.redirect});
    }catch(e){setError(e instanceof Error?e.message:'Unable to complete the request');setBusy('')}
  }
  const user=session.data?.data.user;const who=user?.name||user?.username;
  if(done){
    const allowed=done.kind==='allow';const name=data?.client.name??'your client';
    return <div className="login-page consent-page">
      <LoginBackdrop/>
      <main className="login-stage">
        <div className="login-card consent-card consent-done" role="status">
          <div className="login-head">
            <div className="brand"><Logo/><span>kaflux<i>.</i></span></div>
            <span className="consent-mark" data-outcome={done.kind} aria-hidden="true">{allowed?<Check size={22}/>:<X size={22}/>}</span>
            <h1><span className="login-shine">{allowed?'Authentication':'Request'}</span> <span className="login-serif">{allowed?'successful.':'denied.'}</span></h1>
            <p>{allowed?<>{name} is now connected to Kaflux. You can close this page and return to it.</>:<>{name} was not given access. You can close this page.</>}</p>
          </div>
          <a className="consent-continue" href={done.redirect}>Continue to {name}</a>
        </div>
      </main>
    </div>;
  }
  const offered=SCOPES.filter(s=>data?.scopes.includes(s.id));
  return <div className="login-page consent-page">
    <LoginBackdrop/>
    <main className="login-stage">
      <div className="login-card consent-card" ref={card}>
        <div className="login-head">
          <div className="brand"><Logo/><span>kaflux<i>.</i></span></div>
          {request.isError?<>
            <h1><span className="login-shine">{blocked?'You can\'t approve':'This request'}</span> <span className="login-serif">{blocked?'this request.':"can't continue."}</span></h1>
            <p>{blocked?request.error.message:'The link expired or was already used. Start the connection again from your AI client.'}</p>
          </>:<>
            <h1><span className="login-shine">Allow access to</span> <span className="login-serif">Kaflux?</span></h1>
            <p>{who?<>Signed in as <strong>{who}</strong>. </>:null}Choose what this client may do on your behalf.</p>
          </>}
        </div>
        {!request.isError&&(!data?<div className="consent-loading" role="status">Loading request…</div>:<>
          <div className="consent-client"><span className="consent-avatar" aria-hidden="true">{initials(data.client.name)}</span><div><strong>{data.client.name}</strong><small>{data.client.host}</small></div></div>
          <fieldset className="consent-scopes">
            <legend>Access requested</legend>
            {offered.map(s=>{const on=chosen.includes(s.id);const locked=!data.ownerScopes.includes(s.id);return <label key={s.id} className="consent-scope" data-scope={s.id} data-on={on&&!locked} data-locked={locked||undefined}>
              <input type="checkbox" checked={on&&!locked} disabled={locked||busy!==''} onChange={()=>setScopes(toggle(chosen,s.id))}/>
              <span className="consent-text"><strong>{s.label}</strong><small>{locked?'Your account does not allow this, so it will not be granted.':s.description}</small></span>
              <span className="consent-box" aria-hidden="true"><Check size={12}/></span>
            </label>})}
          </fieldset>
          {data.clusters.length>1&&<fieldset className="consent-clusters"><legend>Clusters</legend>
            <p>Leave all unchecked to allow every cluster you can access.</p>
            <div>{data.clusters.map(c=><label key={c.id}><input type="checkbox" checked={allowed.includes(c.id)} disabled={busy!==''} onChange={()=>setAllowed(toggle(allowed,c.id))}/>{c.name}</label>)}</div>
          </fieldset>}
          <p className="consent-terms"><ShieldCheck size={14} aria-hidden="true"/><span>The client gets a token that lasts {data.accessTokenMinutes>=60?`${Math.round(data.accessTokenMinutes/60)} hour`:`${data.accessTokenMinutes} minutes`} and renews for up to {data.refreshTokenDays} days. You can revoke it any time in MCP access.</span></p>
          {error&&<p className="login-error" role="alert"><CircleAlert size={14} aria-hidden="true"/>{error}</p>}
          <div className="consent-actions">
            <button type="button" className="consent-deny" disabled={busy!==''} onClick={()=>decide('deny')}>{busy==='deny'?'Denying…':'Deny'}</button>
            <button type="button" className="login-submit consent-allow" aria-busy={busy==='allow'} disabled={busy!==''||chosen.length===0} onClick={()=>decide('allow')}><span>{busy==='allow'?'Allowing…':'Allow access'}</span><i className="login-chip"><ArrowRight size={15} aria-hidden="true"/></i></button>
          </div>
        </>)}
      </div>
    </main>
  </div>;
}
