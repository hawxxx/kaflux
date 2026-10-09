export const topicSortKeys=['name','partitions','replicationFactor','cleanupPolicy','sizeBytes','urp'] as const;
/** Topic detail tabs; a topic link may open one directly. */
export const topicTabs=['Overview','Partitions','Distribution','Metrics','Messages','Consumers','Configuration','Balance','Audit'] as const;
export type TopicSearch = {q:string;sort:typeof topicSortKeys[number];order:'asc'|'desc';page:number;showSize:boolean;showInternal:boolean;planTopic?:string;group?:string;tab?:typeof topicTabs[number]};

/** Shared table state is bounded before it reaches Kafka inventory requests. */
export function validateTopicSearch(search:Record<string,unknown>):TopicSearch {
  const page=typeof search.page==='number'?search.page:typeof search.page==='string'&&/^\d+$/.test(search.page)?Number(search.page):0;
  return {
    q:typeof search.q==='string'?search.q.slice(0,256):'',
    sort:topicSortKeys.find(key=>key===search.sort)??'name',
    order:search.order==='desc'?'desc':'asc',
    page:Number.isSafeInteger(page)&&page>=0?Math.min(page,10000):0,
    showSize:search.showSize!==false&&search.showSize!=='false',
    showInternal:search.showInternal!==false&&search.showInternal!=='false',
    // Preselects a topic when a topic page hands off to the reassignment planner.
    ...(topicTabs.find(t=>t===search.tab)?{tab:search.tab as TopicSearch['tab']}:{}),
    ...(typeof search.group==='string'&&search.group?{group:search.group.slice(0,249)}:{}),
    ...(typeof search.planTopic==='string'&&search.planTopic?{planTopic:search.planTopic.slice(0,249)}:{}),
  };
}
