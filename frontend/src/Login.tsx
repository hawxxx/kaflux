import {useRef,useState,type PointerEvent as ReactPointerEvent} from 'react';
import {useQuery} from '@tanstack/react-query';
import {ArrowRight,CircleAlert,Eye,EyeOff,LockKeyhole} from 'lucide-react';
import '@fontsource-variable/geist';
import '@fontsource/newsreader/400-italic.css';
import {api} from './api';
import {Logo} from './Logo';
import {LoginBackdrop,signalBackdrop} from './LoginBackdrop';
import {providerIcon} from './provider-icons';
import './login.css';

type Provider={id:string;name:string;type:string};

const ssoErrors:Record<string,string>={sso_state:'Single sign-on expired or was started in another browser. Try again.',sso_denied:'The identity provider did not approve the sign-in.',sso_rejected:'Your identity was verified but has no Kaflux role. Ask an administrator to map one of your groups.'};

/** Release builds read "v0.6.2"; main builds "main-<sha>" shorten to seven characters; anything else is shown as reported. */
export function formatVersion(version:string|undefined):string|undefined{
  const value=version?.trim();
  if(!value)return undefined;
  if(/^\d+\.\d+\.\d+([-+].*)?$/.test(value))return `v${value}`;
  const main=/^main-([0-9a-f]{7})[0-9a-f]*$/i.exec(value);
  return main?`main-${main[1]}`:value;
}

function SsoButton({provider}:{provider:Provider}){
  const icon=providerIcon(provider.id,provider.name);
  return <a className="login-sso" aria-label={`Continue with ${provider.name}`} href={`/api/v1/auth/oidc/${encodeURIComponent(provider.id)}/start`} style={icon?{['--brand' as string]:icon.color}:undefined}>
    {icon?<svg className="login-sso-mark" viewBox="0 0 24 24" aria-hidden="true"><path d={icon.path}/></svg>:<LockKeyhole size={16} aria-hidden="true"/>}
    <span className="login-sso-name"><span className="login-sso-prefix">Continue with </span>{provider.name}</span>
  </a>;
}

export function Login({onSuccess}:{onSuccess:()=>void}){
  const providers=useQuery({queryKey:['/auth/providers'],queryFn:()=>api<Provider[]>('/auth/providers')});
  // Absent when the operator sets hideVersion; the footer then simply omits it.
  const version=useQuery({queryKey:['/version'],queryFn:()=>api<{version:string}>('/version'),retry:false,staleTime:Infinity});
  const [username,setUsername]=useState('');
  const [password,setPassword]=useState('');
  const [reveal,setReveal]=useState(false);
  const [error,setError]=useState(()=>ssoErrors[new URLSearchParams(window.location.search).get('error')??'']??'');
  const [busy,setBusy]=useState(false);
  const card=useRef<HTMLDivElement>(null);
  const list=providers.data?.data??[];
  const passwordLogin=!providers.data||list.some(p=>p.type!=='oidc');
  const sso=list.filter(p=>p.type==='oidc');
  // A loaded but empty list means no account or provider exists, so a form could never succeed.
  const unconfigured=providers.isSuccess&&list.length===0;
  const shown=formatVersion(version.data?.data.version);
  const spotlight=(e:ReactPointerEvent<HTMLDivElement>)=>{const el=card.current;if(!el)return;const r=el.getBoundingClientRect();el.style.setProperty('--mx',`${e.clientX-r.left}px`);el.style.setProperty('--my',`${e.clientY-r.top}px`)};
  const lean=(e:ReactPointerEvent<HTMLButtonElement>)=>{const el=e.currentTarget,r=el.getBoundingClientRect();el.style.transform=`translate(${((e.clientX-r.left)/r.width-.5)*6}px,${((e.clientY-r.top)/r.height-.5)*4-1}px)`};
  return <div className="login-page">
    <LoginBackdrop/>
    <main className="login-stage">
      <div className="login-card" ref={card} onPointerMove={spotlight}>
        <div className="login-head">
          <div className="brand"><Logo/><span>kaflux<i>.</i></span></div>
          <h1><span className="login-shine">Every stream,</span> <span className="login-serif">one console.</span></h1>
          <p>Sign in to continue.</p>
        </div>
        {error&&<div className="login-error" role="alert"><CircleAlert size={15} aria-hidden="true"/>{error}</div>}
        {unconfigured&&<div className="login-notice" role="status"><strong>No sign-in method is configured.</strong><span>Set a local administrator with <code>KAFLUX_ADMIN_USER</code> and <code>KAFLUX_ADMIN_PASSWORD_HASH</code>, or configure an identity provider, then reload this page.</span><a href="https://github.com/hawxxx/kaflux/blob/main/docs/authentication.md" target="_blank" rel="noreferrer noopener">Authentication guide</a></div>}
        {passwordLogin&&!unconfigured&&<form onInput={()=>signalBackdrop('input')} onSubmit={async e=>{e.preventDefault();if(busy)return;setBusy(true);setError('');signalBackdrop('submit');try{await api('/auth/login',{method:'POST',body:JSON.stringify({username,password})});onSuccess()}catch(e){setError((e as Error).message);signalBackdrop('error')}finally{setBusy(false)}}}>
          <label className="login-field">Username<input autoComplete="username" spellCheck={false} value={username} onChange={e=>setUsername(e.target.value)} required/></label>
          <label className="login-field">Password<span className="login-control"><input type={reveal?'text':'password'} autoComplete="current-password" value={password} onChange={e=>setPassword(e.target.value)} required/><button type="button" className="login-reveal" aria-label={reveal?'Hide password':'Show password'} aria-pressed={reveal} onClick={()=>setReveal(v=>!v)}>{reveal?<EyeOff size={16}/>:<Eye size={16}/>}</button></span></label>
          <button className="login-submit" disabled={busy} aria-busy={busy} onPointerMove={lean} onPointerLeave={e=>{e.currentTarget.style.transform=''}}>
            <span>{busy?'Signing in…':'Sign in'}</span>
            <span className="login-chip" aria-hidden="true">{busy?<span className="login-spin"/>:<ArrowRight size={15}/>}</span>
          </button>
        </form>}
        {passwordLogin&&sso.length>0&&<div className="login-or">or</div>}
        {sso.length>0&&<div className={sso.length>1?'login-sso-list several':'login-sso-list'}>{sso.map(p=><SsoButton key={p.id} provider={p}/>)}</div>}
        {shown&&<div className="login-foot"><span>Kaflux {shown}</span></div>}
      </div>
    </main>
  </div>;
}
