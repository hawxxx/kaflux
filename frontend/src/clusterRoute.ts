// The URL carries no cluster id until the cluster list has loaded and the first cluster is picked.
// Until then the page must not fetch against a placeholder id, or it flashes "cluster not found".
export function awaitingClusterList(urlHasCluster:boolean,listLoaded:boolean,listFailed:boolean){return !urlHasCluster&&!listLoaded&&!listFailed}
