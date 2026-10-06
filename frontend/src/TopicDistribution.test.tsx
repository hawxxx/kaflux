import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {TopicDistribution,type TopicAnalysis} from './TopicDistribution';
const dimension=(id:string,status:'GOOD'|'HIGH_SKEW',cv:number)=>({id,status,mean:2,maxMinusMin:2,coefficientOfVariation:cv,maxToMeanRatio:1.5,brokers:[{broker:1,value:3,differenceFromMean:1,percentageDifference:50},{broker:2,value:1,differenceFromMean:-1,percentageDifference:-50}]});
const analysis:TopicAnalysis={topic:'orders',replicationFactor:2,observedAt:'2026-10-05T12:00:00Z',preferredLeaderRatio:.5,urp:1,offline:0,rackAware:false,singleRackPartitions:0,
  brokers:[{broker:1,rack:'',replicas:2,leaders:2,preferredReplicas:1,outOfSync:0,bytes:200},{broker:2,rack:'',replicas:2,leaders:0,preferredReplicas:1,outOfSync:1,bytes:200}],
  partitions:[{id:0,leader:1,replicas:[1,2],isr:[1,2],sizeBytes:100,preferredLeader:true,underReplicated:false,offline:false,singleRack:false},{id:1,leader:1,replicas:[2,1],isr:[1],sizeBytes:100,preferredLeader:false,underReplicated:true,offline:false,singleRack:false}],
  dimensions:[dimension('replicas','GOOD',0),dimension('leaders','HIGH_SKEW',1),{...dimension('bytes','GOOD',0)}],
  findings:[{severity:'warning',kind:'under-replicated',message:'Partition 1 has 1 of 2 replicas in sync',partition:1}]};
afterEach(()=>vi.restoreAllMocks());
describe('topic distribution analysis',()=>{
  it('shows per-broker placement, replica roles and findings, and re-analyzes on demand',async()=>{
    const fetch=vi.fn(async(_path:string)=>({ok:true,json:async()=>({data:analysis})}) as Response);vi.stubGlobal('fetch',fetch);
    render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><TopicDistribution clusterId="demo" topic="orders"/></QueryClientProvider>);
    expect(await screen.findByText('Partition 1 has 1 of 2 replicas in sync')).toBeInTheDocument();
    expect(String(fetch.mock.calls[0][0])).toContain('/clusters/demo/topics/orders/balance');
    expect(screen.getByText('High skew')).toBeInTheDocument();
    expect(screen.getAllByLabelText('Out of sync, preferred leader')).toHaveLength(1);
    expect(screen.getAllByLabelText('Leader, preferred leader')).toHaveLength(1);
    expect(screen.getByText('ISR 1/2')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button',{name:'Analyze'}));
    await waitFor(()=>expect(fetch).toHaveBeenCalledTimes(2));
  });
});
