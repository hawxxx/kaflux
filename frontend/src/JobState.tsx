import {AlertTriangle} from 'lucide-react';

const tone:Record<string,string>={planned:'planned',queued:'queued',running:'running','rollback-queued':'rollback',paused:'paused',completed:'completed',failed:'failed',canceled:'canceled'};

/** The colored state pill of a rebalance job. A queued rollback reads as rollback-queued. */
export function JobState({state,warnings,rollbackOf}:{state:string;warnings?:string[];rollbackOf?:string}){
  const label=state==='queued'&&rollbackOf?'rollback-queued':state;
  const warned=state==='completed'&&!!warnings?.length;
  return <span className={`job-state ${tone[label]??'canceled'}`} title={warned?warnings!.join('\n'):undefined}>
    <i aria-hidden="true"/>{label}{warned&&<AlertTriangle size={11} className="job-state-warning" aria-label={`${warnings!.length} warnings`}/>}
  </span>;
}
