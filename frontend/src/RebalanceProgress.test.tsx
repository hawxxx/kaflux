import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {afterEach,expect,it,vi} from 'vitest';
import {RebalanceProgress} from './RebalanceProgress';
import type {RebalanceJob} from './rebalance';

afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals()});
// Calls to rebalance actions only; the health strip also fetches cluster data.
const actions=(f:ReturnType<typeof vi.fn>)=>f.mock.calls.filter(c=>String(c[0]).includes('/rebalances/'));
const GiB=1024**3;
const job={id:'job-1',state:'running',planHash:'reviewed',topics:['a','b','c'],changes:[],before:null,after:null,createdAt:'',
  throttleBytesPerSec:10485760,progress:63,etaSeconds:2460,etaBasis:'bytes',rateBytesPerSec:10276044,partitionsDone:214,partitionsTotal:340,bytesDone:30*GiB,bytesTotal:48*GiB,currentStep:1,startedAt:new Date(Date.now()-3600_000).toISOString(),
  steps:[{topic:'orders',state:'done',partitions:24,partitionsDone:24,bytes:3*GiB},{topic:'invoices',state:'moving',partitions:24,partitionsDone:18,bytes:3*GiB,bytesDone:2.25*GiB},{topic:'shipments',state:'pending',partitions:36,partitionsDone:0}]} as RebalanceJob;

it('shows overall progress, what is left and each topic',()=>{
  render(<RebalanceProgress clusterId="demo" job={job} canAct onChange={()=>{}}/>);
  expect(screen.getByText('63%')).toBeVisible();
  expect(screen.getByText('41m00s')).toBeVisible();
  expect(screen.getByText('1 / 3')).toBeVisible();
  expect(screen.getByText('2 left')).toBeVisible();
  expect(screen.getByText('214 / 340')).toBeVisible();
  expect(screen.getByText('126 left')).toBeVisible();
  expect(screen.getByText('18/24')).toBeVisible();
  expect(screen.getByText('75%')).toBeVisible();
  expect(screen.getByText('done · leaders elected')).toBeVisible();
  expect(screen.getByRole('progressbar',{name:'Rebalance progress'})).toHaveAttribute('aria-valuenow','63');
});

it('confirms pause before asking the server',async()=>{
  const fetch=vi.fn(async(_input:RequestInfo|URL,_options?:RequestInit)=>({ok:true,json:async()=>({data:{...job,pauseRequested:true}})} as Response));
  vi.stubGlobal('fetch',fetch);
  const onChange=vi.fn();
  render(<RebalanceProgress clusterId="demo" job={job} canAct onChange={onChange}/>);
  fireEvent.click(screen.getByRole('button',{name:'Pause after this topic'}));
  expect(actions(fetch)).toHaveLength(0);
  fireEvent.click(screen.getAllByRole('button',{name:'Pause after this topic'}).at(-1)!);
  await waitFor(()=>expect(onChange).toHaveBeenCalled());
  expect(actions(fetch)[0][0]).toBe('/api/v1/clusters/demo/rebalances/job-1/pause');
  expect(JSON.parse(actions(fetch)[0][1]?.body as string)).toEqual({confirmation:true,planHash:'reviewed',skip:false});
});

it('offers resume and skip with the reason while paused',async()=>{
  const fetch=vi.fn(async(_input:RequestInfo|URL,_options?:RequestInit)=>({ok:true,json:async()=>({data:{...job,state:'running'}})} as Response));
  vi.stubGlobal('fetch',fetch);
  render(<RebalanceProgress clusterId="demo" job={{...job,state:'paused',pauseReason:'Topic invoices failed: boom'}} canAct onChange={()=>{}}/>);
  expect(screen.getByText(/Topic invoices failed: boom/)).toBeVisible();
  expect(screen.queryByRole('button',{name:'Pause after this topic'})).toBeNull();
  fireEvent.click(screen.getByRole('button',{name:'Skip topic'}));
  fireEvent.click(screen.getAllByRole('button',{name:'Skip topic'}).at(-1)!);
  await waitFor(()=>expect(actions(fetch)).toHaveLength(1));
  expect(actions(fetch)[0][0]).toBe('/api/v1/clusters/demo/rebalances/job-1/resume');
  expect(JSON.parse(actions(fetch)[0][1]?.body as string).skip).toBe(true);
});

it('falls back to unknown data size and disables actions without permission',()=>{
  render(<RebalanceProgress clusterId="demo" job={{...job,bytesTotal:undefined,bytesDone:undefined,etaBasis:'topics'}} canAct={false} onChange={()=>{}}/>);
  expect(screen.getByText('Unknown')).toBeVisible();
  expect(screen.getByText('(by topic)')).toBeVisible();
  expect(screen.getByRole('button',{name:'Cancel'})).toBeDisabled();
});
