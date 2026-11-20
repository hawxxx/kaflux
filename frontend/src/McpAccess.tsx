import {useEffect,useRef,useState,type FormEvent,type RefObject} from 'react';
import {useQuery,useQueryClient} from '@tanstack/react-query';
import * as Dialog from '@radix-ui/react-dialog';
import {Check,Copy,KeyRound,RefreshCw,TriangleAlert,X} from 'lucide-react';
import {api,type Session} from './api';
import {LoadingPanel} from './LoadingPanel';
import {NumberStepper} from './NumberStepper';
import {useTimeZone} from './TimeZone';
import './McpAccess.css';

export type McpToken={id:string;name:string;ownerId:string;ownerProvider:string;scopes:string[];clusters:string[];created:string;expires:string;lastUsed:string|null;revoked:string|null;status:'active'|'expired'|'revoked'};
export type McpMeta={enabled:boolean;writesEnabled:boolean;maxActive:number;maxExpiryDays:number};
type Inventory={data:McpToken[];meta:McpMeta};
type Reveal={name:string;secret:string};
type Scope={id:string;label:string;description:string;warning?:string};

const SCOPES:Scope[]=[
  {id:'read',label:'Read',description:'Inspect topics, brokers, consumer groups, configuration, schemas, connectors, metrics and the audit log. No message contents.'},
  {id:'messages',label:'Messages',description:"Read message payloads. Payloads are sent to your AI client's model, so grant this only for data you may share with it."},
  {id:'write',label:'Write',description:'Produce messages and change topics, configuration, ACLs, quotas, schemas and connectors.',warning:'Write access means your AI client can change your clusters. Every change is prepared, shown to you and confirmed before it runs, and it is audited.'},
  {id:'destructive',label:'Destructive',description:'Delete topics, records and consumer groups, reset offsets, and run or roll back rebalance plans.',warning:'Destructive access means your AI client can delete data and move partitions. Grant it only for short-lived tokens you watch closely.'},
];
const DEFAULT_DAYS='30';
const QUERY_KEY=['mcp-tokens'];

/** Whether the user may create tokens and see every user's tokens. The backend enforces this on every request. */
export function canManageMcp(session?:Session){return session?.canManageMcp??session?.permissions?.['*']?.includes('manage-mcp')??false}

/** Connection settings for common MCP clients. Built on demand from the one-time secret and never stored. */
export function connectionSnippets(origin:string,secret:string){
  const url=`${origin}/mcp`;
  return {
    claudeCode:`claude mcp add --transport http kaflux ${url} --header "Authorization: Bearer ${secret}"`,
    desktop:JSON.stringify({mcpServers:{kaflux:{type:'http',url,headers:{Authorization:`Bearer ${secret}`}}}},null,2),
    generic:`URL: ${url}\nTransport: Streamable HTTP\nHeader: Authorization: Bearer ${secret}`,
  };
}

/** Copies text with the Clipboard API, falling back to a hidden text area for browsers or contexts without it. */
export async function copyText(text:string){
  try{if(navigator.clipboard?.writeText){await navigator.clipboard.writeText(text);return true}}catch{/* fall back below */}
  const area=document.createElement('textarea');
  try{
    area.value=text;area.setAttribute('readonly','');area.style.position='fixed';area.style.opacity='0';
    document.body.appendChild(area);area.select();
    return document.execCommand('copy');
  }catch{return false}finally{area.remove()}
}

type Errors={name?:string;scopes?:string;days?:string};
export function validateToken(name:string,scopes:string[],days:string,maxDays:number):Errors{
  const errors:Errors={};const trimmed=name.trim();
  if(!trimmed)errors.name='Enter a token name.';
  else if([...trimmed].length>64)errors.name='Use at most 64 characters.';
  else if(trimmed.includes('/'))errors.name='The name cannot contain "/".';
  else if(/[\u0000-\u001f\u007f-\u009f]/.test(trimmed))errors.name='Remove control characters from the name.';
  if(!scopes.length)errors.scopes='Choose at least one scope.';
  const n=Number(days);
  if(!/^\d+$/.test(days)||n<1||n>maxDays)errors.days=`Choose from 1 to ${maxDays} days.`;
  return errors;
}

const statusLabel={active:'Active',expired:'Expired',revoked:'Revoked'};

export function McpAccess({session,clusters}:{session?:Session;clusters:{id:string;name:string}[]}){
  const tz=useTimeZone();const client=useQueryClient();const manager=canManageMcp(session);
  const query=useQuery({queryKey:QUERY_KEY,queryFn:async()=>await api<McpToken[]>('/admin/mcp-tokens') as unknown as Inventory});
  const [reveal,setReveal]=useState<Reveal|null>(null);
  const [selected,setSelected]=useState<McpToken|null>(null);const [busy,setBusy]=useState(false);const [revokeError,setRevokeError]=useState('');const [notice,setNotice]=useState('');
  const createHeading=useRef<HTMLHeadingElement>(null);
  const meta=query.data?.meta;const tokens=query.data?.data??[];
  async function revoke(){
    if(!selected)return;setBusy(true);setRevokeError('');
    try{await api(`/admin/mcp-tokens/${encodeURIComponent(selected.id)}`,{method:'DELETE'});setSelected(null);setNotice('Token revoked.');await client.invalidateQueries({queryKey:QUERY_KEY})}
    catch(e){setRevokeError(e instanceof Error?e.message:'Unable to revoke the token')}
    finally{setBusy(false)}
  }
  function dismiss(){setReveal(null);requestAnimationFrame(()=>createHeading.current?.focus())}
  const activeCount=tokens.filter(t=>t.status==='active').length;
  return <div className="mcp-access">
    <section className="panel mcp-intro"><div className="panel-title"><div><h2>MCP access</h2><p>Personal tokens let AI clients such as Claude use Kaflux through the Model Context Protocol. A token never has more access than its owner, and its scopes and cluster list narrow it further.</p></div><KeyRound size={18} aria-hidden="true" className="mcp-intro-icon"/></div></section>
    {query.isLoading?<LoadingPanel label="Loading MCP tokens"/>:query.error?<section className="panel"><div className="inline-error mcp-load-error" role="alert"><span>Unable to load MCP tokens: {query.error.message}</span><button className="button" onClick={()=>query.refetch()}>Retry</button></div></section>:meta&&<>
      {!meta.enabled&&<div className="capability-notice" data-tone="warn" role="note"><TriangleAlert size={16} aria-hidden="true"/><div><strong>MCP is disabled by the administrator.</strong><p>No AI client can connect until MCP is enabled in the Kaflux configuration. Existing tokens are listed below and can still be revoked.</p></div></div>}
      {reveal&&<SecretReveal reveal={reveal} onDismiss={dismiss}/>}
      {meta.enabled&&(manager
        ?<CreateToken meta={meta} clusters={clusters} headingRef={createHeading} onCreated={async r=>{setNotice('');setReveal(r);await client.invalidateQueries({queryKey:QUERY_KEY})}}/>
        :<div className="capability-notice" role="note"><KeyRound size={16} aria-hidden="true"/><div><strong>Token creation is not available to you</strong><p>Creating MCP tokens requires the manage-mcp permission. Ask an administrator to grant it. You can still review and revoke your own tokens here.</p></div></div>)}
      <section className="panel mcp-tokens" aria-labelledby="mcp-tokens-title">
        <div className="panel-title"><div><h2 id="mcp-tokens-title">{manager?'All MCP tokens':'Your MCP tokens'}</h2><p>{manager?'Tokens of every user. ':''}Up to {meta.maxActive} active tokens per user. Revoking a token disconnects its clients immediately.</p></div><button className="button" disabled={query.isFetching||busy} onClick={()=>query.refetch()}><RefreshCw size={13} aria-hidden="true"/>Refresh</button></div>
        {tokens.length?<div className="mcp-table-scroll"><table className="mcp-token-table"><thead><tr><th scope="col">Name</th>{manager&&<th scope="col">Owner</th>}<th scope="col">Scopes</th><th scope="col">Clusters</th><th scope="col">Status</th><th scope="col">Created</th><th scope="col">Expires</th><th scope="col">Last used</th><th scope="col"><span className="visually-hidden">Actions</span></th></tr></thead>
          <tbody>{tokens.map(t=><tr key={t.id} data-status={t.status}>
            <td data-label="Name"><strong className="mcp-token-name">{t.name}</strong></td>
            {manager&&<td data-label="Owner"><span className="mcp-owner">{t.ownerId}<small>{t.ownerProvider}</small></span></td>}
            <td data-label="Scopes"><span className="mcp-chips">{t.scopes.map(s=><span key={s} className="tag" data-scope={s}>{s}</span>)}</span></td>
            <td data-label="Clusters">{t.clusters.length?<span className="mcp-chips">{t.clusters.map(c=><span key={c} className="tag">{c}</span>)}</span>:<span className="muted">All permitted clusters</span>}</td>
            <td data-label="Status"><span className="mcp-status" data-status={t.status}>{statusLabel[t.status]??t.status}</span></td>
            <td data-label="Created"><time dateTime={t.created}>{tz.dateTime(t.created)}</time></td>
            <td data-label="Expires"><time dateTime={t.expires}>{tz.dateTime(t.expires)}</time></td>
            <td data-label="Last used">{t.lastUsed?<time dateTime={t.lastUsed}>{tz.dateTime(t.lastUsed)}</time>:<span className="muted">Never</span>}</td>
            <td data-label="Actions" className="mcp-actions">{t.status==='active'&&<button className="button danger-button" aria-label={`Revoke token ${t.name}`} disabled={busy} onClick={()=>{setSelected(t);setRevokeError('');setNotice('')}}>Revoke</button>}</td>
          </tr>)}</tbody></table></div>
          :<div className="empty"><KeyRound size={28} aria-hidden="true"/><p>No MCP tokens yet.{manager&&meta.enabled?' Create one above to connect an AI client.':''}</p></div>}
        <div className="panel-bottom"><span>{tokens.length} tokens · {activeCount} active</span></div>
        <p className="mcp-notice" role="status">{notice}</p>
      </section>
    </>}
    <Dialog.Root open={!!selected} onOpenChange={open=>{if(!open&&!busy)setSelected(null)}}><Dialog.Portal><Dialog.Overlay className="dialog-overlay"/><Dialog.Content className="dialog">
      <Dialog.Title>Revoke MCP token</Dialog.Title>
      <Dialog.Description>Revoke the token “{selected?.name}”{manager&&selected?` owned by ${selected.ownerId}`:''}? AI clients using it lose access immediately. This cannot be undone.</Dialog.Description>
      <Dialog.Close className="dialog-close" disabled={busy} aria-label="Close"><X size={18}/></Dialog.Close>
      {revokeError&&<p className="inline-error" role="alert">Unable to revoke the token: {revokeError}. Try again.</p>}
      <div className="wizard-actions"><button className="button" disabled={busy} onClick={()=>setSelected(null)}>Cancel</button><button className="button danger-button" disabled={busy} onClick={revoke}>{busy?'Revoking…':'Confirm revocation'}</button></div>
    </Dialog.Content></Dialog.Portal></Dialog.Root>
  </div>;
}

function CreateToken({meta,clusters,headingRef,onCreated}:{meta:McpMeta;clusters:{id:string;name:string}[];headingRef:RefObject<HTMLHeadingElement|null>;onCreated:(reveal:Reveal)=>Promise<void>}){
  const [name,setName]=useState('');const [scopes,setScopes]=useState<string[]>(['read']);const [allowed,setAllowed]=useState<string[]>([]);const [days,setDays]=useState(DEFAULT_DAYS);
  const [errors,setErrors]=useState<Errors>({});const [submitError,setSubmitError]=useState('');const [busy,setBusy]=useState(false);
  const nameRef=useRef<HTMLInputElement>(null);const scopeRef=useRef<HTMLInputElement>(null);const daysRef=useRef<HTMLDivElement>(null);
  const toggle=(list:string[],value:string)=>list.includes(value)?list.filter(x=>x!==value):[...list,value];
  async function submit(event:FormEvent){
    event.preventDefault();setSubmitError('');
    const found=validateToken(name,scopes,days,meta.maxExpiryDays);setErrors(found);
    if(found.name){nameRef.current?.focus();return}
    if(found.scopes){scopeRef.current?.focus();return}
    if(found.days){daysRef.current?.querySelector('input')?.focus();return}
    setBusy(true);
    try{
      const ordered=SCOPES.map(s=>s.id).filter(s=>scopes.includes(s));
      const body:{name:string;scopes:string[];clusters?:string[];expiresInDays:number}={name:name.trim(),scopes:ordered,expiresInDays:Number(days)};
      if(allowed.length)body.clusters=clusters.map(c=>c.id).filter(c=>allowed.includes(c));
      // A plain request, not a cached query or mutation, so the one-time secret lives only in component state.
      const result=await api<McpToken&{secret:string}>('/admin/mcp-tokens',{method:'POST',body:JSON.stringify(body)});
      const reveal={name:result.data.name,secret:result.data.secret};
      setName('');setScopes(['read']);setAllowed([]);setDays(DEFAULT_DAYS);setErrors({});
      await onCreated(reveal);
    }catch(e){setSubmitError(e instanceof Error?e.message:'Unable to create the token')}
    finally{setBusy(false)}
  }
  return <section className="panel mcp-create" aria-labelledby="mcp-create-title">
    <div className="panel-title"><div><h2 id="mcp-create-title" ref={headingRef} tabIndex={-1}>Create token</h2><p>The secret is shown once after creation. Kaflux keeps only a hash of it.</p></div></div>
    <form className="mcp-form" onSubmit={submit} noValidate>
      <div className="mcp-field">
        <label htmlFor="mcp-token-name">Token name</label>
        <input id="mcp-token-name" ref={nameRef} value={name} maxLength={64} autoComplete="off" placeholder="e.g. laptop-claude" aria-invalid={!!errors.name||undefined} aria-describedby={errors.name?'mcp-token-name-error':'mcp-token-name-hint'} onChange={e=>setName(e.target.value)}/>
        {errors.name?<p id="mcp-token-name-error" className="mcp-field-error">{errors.name}</p>:<p id="mcp-token-name-hint" className="mcp-hint">Up to 64 characters, without "/". Unique among your active tokens.</p>}
      </div>
      <fieldset className="mcp-fieldset" aria-describedby={errors.scopes?'mcp-scopes-error':undefined}>
        <legend>Scopes</legend>
        {SCOPES.map((s,i)=>{const on=scopes.includes(s.id);return <div key={s.id} className="mcp-scope" data-checked={on}>
          <label className="checkbox-label mcp-scope-label"><input ref={i===0?scopeRef:undefined} type="checkbox" checked={on} onChange={()=>setScopes(toggle(scopes,s.id))}/><span><strong>{s.label}</strong><small>{s.description}</small></span></label>
          {on&&s.warning&&<div className="mcp-scope-warning" role="note"><TriangleAlert size={14} aria-hidden="true"/><p>{s.warning}{!meta.writesEnabled&&<> <em>This scope has no effect until an administrator enables MCP writes.</em></>}</p></div>}
        </div>})}
        {errors.scopes&&<p id="mcp-scopes-error" className="mcp-field-error">{errors.scopes}</p>}
      </fieldset>
      {clusters.length>0&&<fieldset className="mcp-fieldset">
        <legend>Clusters</legend>
        <p className="mcp-hint">Leave all unchecked to allow every cluster you can access, including clusters added later.</p>
        <div className="mcp-cluster-list">{clusters.map(c=><label key={c.id} className="checkbox-label"><input type="checkbox" checked={allowed.includes(c.id)} onChange={()=>setAllowed(toggle(allowed,c.id))}/>{c.name}{c.name!==c.id&&<small className="muted"> ({c.id})</small>}</label>)}</div>
      </fieldset>}
      <div className="mcp-field" ref={daysRef}>
        <label htmlFor="mcp-token-expiry">Expires in days</label>
        <NumberStepper id="mcp-token-expiry" label="expiry" value={days} onChange={setDays} min={1} max={meta.maxExpiryDays} invalid={!!errors.days} describedBy={errors.days?'mcp-expiry-error':'mcp-expiry-hint'} valueText={d=>`${d} ${d===1?'day':'days'}`}/>
        {errors.days?<p id="mcp-expiry-error" className="mcp-field-error">{errors.days}</p>:<p id="mcp-expiry-hint" className="mcp-hint">1 to {meta.maxExpiryDays} days. Expired tokens stop working and stay listed.</p>}
      </div>
      {submitError&&<p className="inline-error" role="alert">Unable to create the token: {submitError}</p>}
      <div className="mcp-form-actions"><button type="submit" className="button primary" disabled={busy}>{busy?'Creating…':'Create token'}</button></div>
    </form>
  </section>;
}

function SecretReveal({reveal,onDismiss}:{reveal:Reveal;onDismiss:()=>void}){
  const heading=useRef<HTMLHeadingElement>(null);const [copied,setCopied]=useState<{label:string;ok:boolean}|null>(null);
  useEffect(()=>{heading.current?.focus()},[reveal]);
  const snippets=connectionSnippets(window.location.origin,reveal.secret);
  async function copy(label:string,text:string){const ok=await copyText(text);setCopied({label,ok})}
  const block=(id:string,title:string,label:string,text:string)=><div className="mcp-snippet"><div className="mcp-snippet-head"><strong>{title}</strong><button type="button" className="button" aria-label={`Copy ${label}`} onClick={()=>copy(label,text)}><Copy size={13} aria-hidden="true"/>Copy</button></div><pre data-testid={`snippet-${id}`}><code>{text}</code></pre></div>;
  return <section className="panel mcp-reveal" aria-labelledby="mcp-reveal-title">
    <div className="panel-title"><div><h2 id="mcp-reveal-title" ref={heading} tabIndex={-1}>Copy your token now</h2><p>Token “{reveal.name}” is ready.</p></div></div>
    <div className="mcp-reveal-body">
      <div className="mcp-scope-warning" role="note"><TriangleAlert size={14} aria-hidden="true"/><p>This secret is shown once. Kaflux stores only a hash, so it cannot show it again. If you lose it, revoke the token and create a new one. Treat it like a password.</p></div>
      <div className="mcp-secret"><code data-testid="mcp-secret">{reveal.secret}</code><button type="button" className="button" aria-label="Copy secret" onClick={()=>copy('secret',reveal.secret)}><Copy size={13} aria-hidden="true"/>Copy</button></div>
      {block('claude-code','Claude Code','Claude Code command',snippets.claudeCode)}
      {block('desktop','Claude Desktop and other JSON clients','Claude Desktop configuration',snippets.desktop)}
      {block('generic','Any MCP client (streamable HTTP)','generic settings',snippets.generic)}
      <p className={`mcp-copy-status${copied&&!copied.ok?' failed':''}`} role="status">{copied?(copied.ok?<><Check size={13} aria-hidden="true"/>{copied.label==='secret'?'Secret':copied.label[0].toUpperCase()+copied.label.slice(1)} copied.</>:'Copy failed. Select the text and copy it manually.'):''}</p>
      <div className="mcp-form-actions"><button type="button" className="button primary" onClick={onDismiss}>I have saved the token</button></div>
    </div>
  </section>;
}
