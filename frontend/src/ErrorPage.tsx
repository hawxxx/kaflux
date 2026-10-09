import {ArrowRight,RefreshCw} from 'lucide-react';

const copy:Record<number,{title:string;message:string}>={
  403:{title:'Access denied',message:'Your account does not have permission to view this page.'},
  404:{title:'Page not found',message:'This page does not exist, or the cluster or topic was renamed or removed.'},
  500:{title:'Something went wrong',message:'The console hit an unexpected error. Reloading usually clears it.'},
};

export function ErrorPage({code=500,title,message,detail,fullPage=false}:{code?:number;title?:string;message?:string;detail?:string;fullPage?:boolean}){
  const text=copy[code]??copy[500];
  return <div className={fullPage?'error-page error-page-full':'error-page'} role={code>=500?'alert':undefined}>
    <p className="error-code mono">{code}</p>
    <h1>{title??text.title}</h1>
    <p>{message??text.message}</p>
    {detail&&<pre className="error-detail mono">{detail}</pre>}
    <div className="error-actions">
      <a className="button primary" href="/">Back to console <ArrowRight size={14}/></a>
      {code>=500&&<button className="button" onClick={()=>window.location.reload()}><RefreshCw size={14}/>Reload</button>}
    </div>
  </div>;
}
