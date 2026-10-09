import {Layers3,Shield} from 'lucide-react';
export function ErrorBox({error}:{error:Error|null}){return error?<div className="error-box" role="alert"><Shield size={17}/><div><strong>Unable to load this resource</strong><p>{error.message}</p></div></div>:null}
export function Empty({text='No records match this view.'}:{text?:string}){return <div className="empty"><Layers3 size={28}/><p>{text}</p></div>}
export function Loading(){return <div className="loading" role="status"><span aria-hidden="true"/>Retrieving cluster state…</div>}
