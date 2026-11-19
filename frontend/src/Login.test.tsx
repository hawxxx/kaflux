import {afterEach,describe,expect,it,vi} from 'vitest';
import {render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {Login,formatVersion} from './Login';

const respond=(data:unknown,status=200)=>new Response(JSON.stringify(status<400?{data}:{error:{code:'not_found',message:'Not found'}}),{status,headers:{'Content-Type':'application/json'}});

function mount(providers:unknown[]|null,version:{version:string}|null){
  vi.stubGlobal('fetch',vi.fn(async(input:RequestInfo|URL)=>{
    const url=String(input);
    if(url.endsWith('/auth/providers'))return providers?respond(providers):respond(null,503);
    if(url.endsWith('/version'))return version?respond(version):respond(null,404);
    return respond(null,404);
  }));
  const client=new QueryClient({defaultOptions:{queries:{retry:false}}});
  return render(<QueryClientProvider client={client}><Login onSuccess={()=>{}}/></QueryClientProvider>);
}

afterEach(()=>{vi.unstubAllGlobals()});

describe('formatVersion',()=>{
  it('prefixes releases, shortens main builds and keeps other builds as reported',()=>{
    expect(formatVersion('0.6.2')).toBe('v0.6.2');
    expect(formatVersion('1.0.0-rc.1')).toBe('v1.0.0-rc.1');
    expect(formatVersion('main-abfefcf0123456789abcdef0123456789abcdef0')).toBe('main-abfefcf');
    expect(formatVersion('dev')).toBe('dev');
    expect(formatVersion('')).toBeUndefined();
    expect(formatVersion(undefined)).toBeUndefined();
  });
});

describe('Login',()=>{
  it('shows the running version and a branded single sign-on button',async()=>{
    mount([{id:'local',name:'Local account',type:'local'},{id:'authentik',name:'Authentik',type:'oidc'}],{version:'0.6.2'});
    expect(screen.getByRole('heading',{name:'Every stream, one console.'})).toBeVisible();
    expect(await screen.findByText('Kaflux v0.6.2')).toBeVisible();
    const sso=await screen.findByRole('link',{name:'Continue with Authentik'});
    expect(sso).toHaveAttribute('href','/api/v1/auth/oidc/authentik/start');
    expect(sso.querySelector('.login-sso-mark')).not.toBeNull();
    expect(screen.getByLabelText('Password')).toHaveAttribute('type','password');
  });

  it('offers only single sign-on and omits a withheld version',async()=>{
    mount([{id:'corp-sso',name:'Corporate SSO',type:'oidc'}],null);
    await screen.findByRole('link',{name:'Continue with Corporate SSO'});
    expect(screen.queryByLabelText('Username')).toBeNull();
    expect(screen.queryByText('or')).toBeNull();
    await waitFor(()=>expect(fetch).toHaveBeenCalledWith('/api/v1/version',expect.anything()));
    expect(screen.queryByText(/^Kaflux /)).toBeNull();
  });

  it('explains when no sign-in method is configured instead of offering a form that cannot succeed',async()=>{
    mount([],{version:'0.6.2'});
    expect(await screen.findByText('No sign-in method is configured.')).toBeVisible();
    expect(screen.getByRole('link',{name:'Authentication guide'})).toHaveAttribute('href','https://github.com/hawxxx/kaflux/blob/main/docs/authentication.md');
    expect(screen.queryByLabelText('Username')).toBeNull();
    expect(screen.queryByRole('button',{name:'Sign in'})).toBeNull();
  });

  it('keeps the form when sign-in options cannot load and never flashes it while they load',async()=>{
    mount(null,null);
    expect(screen.queryByLabelText('Username')).toBeNull();
    expect(screen.getByLabelText('Loading sign-in options')).toBeInTheDocument();
    expect(await screen.findByRole('alert',{},{timeout:4000})).toHaveTextContent('Sign-in options could not be loaded');
    expect(screen.getByLabelText('Username')).toBeVisible();
    expect(screen.getByRole('button',{name:'Sign in'})).toBeVisible();
  });
});
