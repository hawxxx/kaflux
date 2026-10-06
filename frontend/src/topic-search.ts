export const topicSortKeys=['name','partitions','replicationFactor','cleanupPolicy','sizeBytes','urp'] as const;
export type TopicSearch = {q:string;sort:typeof topicSortKeys[number];order:'asc'|'desc';page:number;showSize:boolean};

/** Shared table state is bounded before it reaches Kafka inventory requests. */
export function validateTopicSearch(search:Record<string,unknown>):TopicSearch {
  const page=typeof search.page==='number'?search.page:typeof search.page==='string'&&/^\d+$/.test(search.page)?Number(search.page):0;
  return {
    q:typeof search.q==='string'?search.q.slice(0,256):'',
    sort:topicSortKeys.find(key=>key===search.sort)??'name',
    order:search.order==='desc'?'desc':'asc',
    page:Number.isSafeInteger(page)&&page>=0?Math.min(page,10000):0,
    showSize:search.showSize!==false&&search.showSize!=='false',
  };
}
