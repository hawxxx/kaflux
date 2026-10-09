import {fireEvent,render,screen} from '@testing-library/react';
import {expect,it} from 'vitest';
import {BalanceEstimate} from './BalanceEstimate';
import {brokerValues,cv,jobNote,type RebalanceJob} from './rebalance';

const before=[1,2,3,4,5,6,7].map(b=>({broker:b,replicas:b<4?20:0,leaders:b<4?10:0,bytes:b<4?2000:0}));
const after=[1,2,3,4,5,6,7].map(b=>({broker:b,replicas:9,leaders:4,bytes:900}));

it('computes CV like the balance advisor and counts missing brokers as zero',()=>{
  expect(cv([10,10,10])).toBe(0);
  expect(cv([0,20])).toBeCloseTo(100);
  const v=brokerValues([{broker:1,replicas:4,leaders:2}],[{broker:1,replicas:2,leaders:1},{broker:2,replicas:2,leaders:1}],'replicas');
  expect(v).toEqual({ids:[1,2],before:[4,0],after:[2,2],known:true});
  expect(brokerValues([{broker:1,replicas:1,leaders:1}],[],'bytes').known).toBe(false);
});

it('shows one-line skew cards and pages brokers five at a time',()=>{
  render(<BalanceEstimate before={before} after={after}/>);
  expect(screen.getByText('Estimated before → after balance')).toBeInTheDocument();
  fireEvent.click(screen.getByText('Estimated before → after balance'));
  expect(screen.getByText('Data skew · CV')).toBeVisible();
  expect(screen.getAllByText('0.0%').length).toBeGreaterThan(0);
  expect(screen.getByText('1–5 of 7 brokers')).toBeVisible();
  expect(screen.queryByText('broker 6')).toBeNull();
  fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
  expect(screen.getByText('6–7 of 7 brokers')).toBeVisible();
  expect(screen.getByText('broker 6')).toBeVisible();
  expect(screen.getByRole('button',{name:'Next brokers'})).toBeDisabled();
});

it('switches to measured values after completion and hides data skew without sizes',()=>{
  const noBytes=before.map(({bytes:_b,...d})=>d);
  render(<BalanceEstimate before={noBytes} after={after} measured={after}/>);
  expect(screen.getByText('Measured before → after balance')).toBeInTheDocument();
  expect(screen.queryByText('Data skew · CV')).toBeNull();
  expect(screen.getByText('Leader skew · CV')).toBeInTheDocument();
});

it('writes a short note per job state',()=>{
  const base={id:'a',planHash:'h',topics:['a'],changes:[],createdAt:'',partitionsTotal:340,partitionsDone:214,steps:[{topic:'orders',state:'done',partitions:1,partitionsDone:1},{topic:'invoices',state:'moving',partitions:1,partitionsDone:0}],currentStep:1} as unknown as RebalanceJob;
  expect(jobNote({...base,state:'running',etaSeconds:2460})).toBe('Running · topic 2 of 2 (invoices) · 214 of 340 partitions · ETA 41m00s. Original placements are saved for rollback.');
  expect(jobNote({...base,state:'paused',pauseReason:'Paused by alice'})).toBe('Paused · 1 of 2 topics moved. Paused by alice');
});
