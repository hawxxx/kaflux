import {useState} from 'react';
import {UnusedTopics} from './UnusedTopics';
import type {Session} from './api';

export const topicHubTabs=['All topics','Cleanup'] as const;
type Tabs<T extends readonly string[]>={tabs:T;value:T[number];onChange:(tab:T[number])=>void;label:string};
function TabBar<T extends readonly string[]>({tabs,value,onChange,label}:Tabs<T>){
  return <div className="tabs" role="tablist" aria-label={label}>{tabs.map(x=><button key={x} type="button" role="tab" aria-selected={value===x} className={value===x?'active':''} onClick={()=>onChange(x)}>{x}</button>)}</div>;
}
/** Topics page: the topic list plus topic cleanup. */
export function TopicsHub({clusterId,session,topics,initialTab='All topics'}:{clusterId:string;session?:Session;topics:React.ReactNode;initialTab?:(typeof topicHubTabs)[number]}){
  const [tab,setTab]=useState<(typeof topicHubTabs)[number]>(initialTab);
  return <><TabBar tabs={topicHubTabs} value={tab} onChange={setTab} label="Topics sections"/>
    {tab==='All topics'?topics:<UnusedTopics clusterId={clusterId} session={session}/>}</>;
}
