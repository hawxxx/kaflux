import {render,screen,waitFor} from '@testing-library/react';
import {afterEach,expect,it,vi} from 'vitest';
import {JobConsole,tokenize} from './JobConsole';
import type {RebalanceJob} from './rebalance';

afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals()});
const job={id:'job-1',state:'running',planHash:'h',topics:['orders','orders.created'],changes:[],before:null,after:null,createdAt:'',steps:[{topic:'orders.created',state:'moving',partitions:24,partitionsDone:18}]} as RebalanceJob;

it('colors topics, sizes, broker lists and durations',()=>{
  const tokens=tokenize('orders.created 74% · 2.3/3.1 GiB · 9.8 MiB/s · ETA 1m25s · brokers [1,2,3] → [1,2,3,4] · PREFERRED',['orders','orders.created']);
  const kinds=Object.fromEntries(tokens.filter(t=>t.kind).map(t=>[t.text,t.kind]));
  expect(kinds).toEqual({'orders.created':'topic','74%':'num','2.3/3.1 GiB':'num','9.8 MiB/s':'num','1m25s':'dur','[1,2,3]':'key','[1,2,3,4]':'key','PREFERRED':'lit'});
  expect(tokens.map(t=>t.text).join('')).toBe('orders.created 74% · 2.3/3.1 GiB · 9.8 MiB/s · ETA 1m25s · brokers [1,2,3] → [1,2,3,4] · PREFERRED');
});

it('shows the log with glyphs, a spinner on the live line, and download links',async()=>{
  const fetch=vi.fn(async(_input:RequestInfo|URL,_options?:RequestInit)=>({ok:true,json:async()=>({data:{last:2,events:[
    {seq:1,at:'2026-10-08T11:02:15Z',level:'info',message:'✔ orders done in 5m12s'},
    {seq:2,at:'2026-10-08T11:09:16Z',level:'warn',message:'⚠ Leader election for orders failed · retry 1/3'},
  ]}})} as Response));
  vi.stubGlobal('fetch',fetch);
  const {container}=render(<JobConsole clusterId="demo" job={job} active/>);
  await waitFor(()=>expect(screen.getByText(/done in/)).toBeVisible());
  expect(fetch.mock.calls[0][0]).toBe('/api/v1/clusters/demo/rebalances/job-1/events?after=0');
  const lines=container.querySelectorAll('.job-line');
  expect(lines[0].querySelector('.job-glyph')).toHaveTextContent('✔');
  expect(lines[1]).toHaveClass('warn');
  expect(lines[1].querySelector('.braille-spinner')).not.toBeNull();
  expect(screen.getByRole('link',{name:'Download backup'})).toHaveAttribute('href','/api/v1/clusters/demo/rebalances/job-1/backup.json');
  expect(screen.getByRole('link',{name:'Report CSV'})).toHaveAttribute('href','/api/v1/clusters/demo/rebalances/job-1/report.csv');
});

it('stops polling and the spinner once the job finished',async()=>{
  const fetch=vi.fn(async(_input:RequestInfo|URL,_options?:RequestInit)=>({ok:true,json:async()=>({data:{last:1,events:[{seq:1,at:'2026-10-08T11:02:15Z',level:'info',message:'✔ Job completed'}]}})} as Response));
  vi.stubGlobal('fetch',fetch);
  const {container}=render(<JobConsole clusterId="demo" job={{...job,state:'completed'}} active={false}/>);
  expect(screen.queryByRole('log')).toBeNull();
  screen.getByRole('button',{name:/Activity log/}).click();
  await waitFor(()=>expect(screen.getByText(/Job completed/)).toBeVisible());
  expect(container.querySelector('.braille-spinner')).toBeNull();
  expect(fetch).toHaveBeenCalledOnce();
});
