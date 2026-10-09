import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {afterEach,expect,it,vi} from 'vitest';
import {ClusterHealth,latestSum} from './ClusterHealth';

afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals()});
const respond=(data:unknown)=>({ok:true,json:async()=>({data})} as Response);

it('sums the newest sample across brokers',()=>{
  expect(latestSum({status:'available',observedAt:'',series:[{points:[{time:1,value:2},{time:2,value:0.5}]},{values:[[2,1]]}]})).toBe(1.5);
  expect(latestSum({status:'unavailable',observedAt:'',series:[]})).toBeUndefined();
});

it('summarizes health and shows details on demand',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(input:RequestInfo|URL)=>{
    const url=String(input);
    if(url.endsWith('/overview'))return respond({totals:{urp:0,offline:0,controller:1}});
    if(url.endsWith('/metrics/catalog'))return respond([{id:'panel-80-0',title:'Kafka ISR Shrinks Per Second'}]);
    return respond({status:'available',observedAt:'',series:[{points:[{time:1,value:0}]}]});
  }));
  render(<ClusterHealth clusterId="demo" live/>);
  await waitFor(()=>expect(screen.getByText('✓ Cluster preflight passed')).toBeInTheDocument());
  expect(screen.getByText('0 URP · 0 offline · controller stable')).toBeInTheDocument();
  fireEvent.click(screen.getByText('✓ Cluster preflight passed'));
  await waitFor(()=>expect(screen.getByText('0.0/s')).toBeVisible());
  expect(screen.getByText('broker 1')).toBeVisible();
});

it('flags under-replicated partitions and missing metrics',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(input:RequestInfo|URL)=>String(input).endsWith('/overview')?respond({totals:{urp:3,offline:0,controller:1}}):respond([])));
  render(<ClusterHealth clusterId="demo" live={false}/>);
  await waitFor(()=>expect(screen.getByText('⚠ Cluster needs attention')).toBeInTheDocument());
  await waitFor(()=>expect(screen.getByText('metrics not configured')).toBeInTheDocument());
});
