import {useState} from 'react';
import {useQueryClient} from '@tanstack/react-query';
import {api,setCsrf} from './api';

export function SessionControl({demo,onSignedOut}:{demo:boolean;onSignedOut:()=>void}){
  const client=useQueryClient();
  const [busy,setBusy]=useState(false);
  const [error,setError]=useState('');
  if(demo)return <p className="session-note">Demo access is automatic. No personal session is active.</p>;
  async function signOut(){
    setBusy(true);setError('');
    try{
      await api('/auth/logout',{method:'POST'});
      await client.cancelQueries();
      client.clear();setCsrf('');onSignedOut();
    }catch(e){setError(e instanceof Error?e.message:'Unable to sign out. Try again.');setBusy(false)}
  }
  return <div className="session-control"><button className="button" disabled={busy} onClick={signOut}>{busy?'Signing out…':'Sign out'}</button>{error&&<p role="alert">Unable to sign out: {error}. Try again.</p>}</div>;
}
