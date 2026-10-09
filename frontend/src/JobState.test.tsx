import {render,screen} from '@testing-library/react';
import {expect,it} from 'vitest';
import {JobState} from './JobState';
import {ago,activeJob,duration,jobSummary,type RebalanceJob} from './rebalance';

it('gives every job state its own tone',()=>{
  const tones:Record<string,string>={planned:'planned',queued:'queued',running:'running',paused:'paused',completed:'completed',failed:'failed',canceled:'canceled'};
  for(const [state,tone] of Object.entries(tones)){
    const {unmount}=render(<JobState state={state}/>);
    expect(screen.getByText(state)).toHaveClass('job-state',tone);
    unmount();
  }
});
it('marks a queued rollback and completed jobs with warnings',()=>{
  render(<><JobState state="queued" rollbackOf="a1"/><JobState state="completed" warnings={['Leader election for orders failed']}/></>);
  expect(screen.getByText('rollback-queued')).toHaveClass('rollback');
  expect(screen.getByLabelText('1 warnings')).toBeInTheDocument();
});
it('formats durations, ages and the history summary',()=>{
  expect(duration(312)).toBe('5m12s');
  expect(duration(4320)).toBe('1h12m');
  expect(ago(new Date(Date.now()-12*60_000).toISOString())).toBe('12m ago');
  const job={id:'a',state:'running',planHash:'h',topics:['a'],changes:[],before:null,after:null,createdAt:'',etaSeconds:2460,steps:[{topic:'a',state:'done',partitions:1,partitionsDone:1},{topic:'b',state:'moving',partitions:1,partitionsDone:0},{topic:'c',state:'pending',partitions:1,partitionsDone:0}]} as RebalanceJob;
  expect(jobSummary(job)).toBe('2/3 topics · ETA 41m00s');
  expect(activeJob({state:'paused'})).toBe(true);
  expect(activeJob({state:'completed'})).toBe(false);
});
