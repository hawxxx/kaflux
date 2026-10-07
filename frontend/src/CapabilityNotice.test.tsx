import {render,screen} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {CapabilityNotice,RebalancingStatus,reassignmentAccess,withStatusBadges,type ClusterCapabilities} from './CapabilityNotice';
describe('cluster capability preflight',()=>{
  it('explains AWS ownership while intelligent balancing is active',()=>{render(<CapabilityNotice capabilities={{manualReassignmentAllowed:false,rebalancingStatus:'ACTIVE',reason:'MSK intelligent balancing is active.',kind:'msk',brokerType:'express',observedAt:'2026-10-03T00:00:00Z'}}/>);expect(screen.getByText('AWS owns partition balancing')).toBeVisible();expect(screen.getByText(/paused through AWS MSK, outside Kaflux/)).toBeVisible()});
  it('does not imply permission when capability is unknown',()=>{render(<CapabilityNotice/>);expect(screen.getByText(/disabled until capability is known/)).toBeVisible()});
  it('shows the ACTIVE status as a warning badge in the heading and inside the AWS reason',()=>{
    const reason='Amazon MSK intelligent rebalancing is ACTIVE. Manual partition reassignment is blocked by AWS.';
    const {container}=render(<CapabilityNotice capabilities={{manualReassignmentAllowed:false,rebalancingStatus:'ACTIVE',reason,kind:'MSK Express',brokerType:'express.m7g.large',observedAt:'2026-10-03T00:00:00Z'}}/>);
    const badges=[...container.querySelectorAll('.rebalancing-status')];
    expect(badges).toHaveLength(2);
    expect(badges.every(b=>b.getAttribute('data-tone')==='warn')).toBe(true);
    expect(badges[0]).toHaveTextContent('Active');
    expect(container.querySelector('.capability-notice')).toHaveAttribute('data-tone','warn');
    expect(screen.getByRole('status')).toHaveTextContent('intelligent rebalancing is Active. Manual partition reassignment is blocked by AWS.');
  });
  it('uses a neutral badge for unknown status and no warning tone',()=>{
    const {container}=render(<CapabilityNotice capabilities={{manualReassignmentAllowed:false,rebalancingStatus:'UNKNOWN',reason:'AWS denied the request (HTTP 403 AccessDenied).',kind:'MSK',brokerType:'',observedAt:'2026-10-03T00:00:00Z'}}/>);
    expect(container.querySelector('.rebalancing-status')).toHaveAttribute('data-tone','neutral');
    expect(container.querySelector('.capability-notice')).not.toHaveAttribute('data-tone');
  });
});
describe('RebalancingStatus',()=>{
  it('maps known states to tones and keeps unexpected values readable and neutral',()=>{
    const tone=(v?:string)=>{const {container,unmount}=render(<RebalancingStatus value={v}/>);const b=container.querySelector('.rebalancing-status')!;const out=[b.getAttribute('data-tone'),b.textContent];unmount();return out};
    expect(tone('ACTIVE')).toEqual(['warn','Active']);
    expect(tone('paused')).toEqual(['good','Paused']);
    expect(tone('NOT_APPLICABLE')).toEqual(['neutral','Not applicable']);
    expect(tone(undefined)).toEqual(['neutral','Unknown']);
    expect(tone('<b>x</b>')).toEqual(['neutral','<B>X</B>']);
  });
  it('only badges whole words and leaves text without the status untouched',()=>{
    expect(withStatusBadges('Rebalancing is INACTIVE here','ACTIVE')).toBe('Rebalancing is INACTIVE here');
    expect(withStatusBadges('nothing to mark','ACTIVE')).toBe('nothing to mark');
  });
});

describe('reassignmentAccess',()=>{
  const caps=(over:Partial<ClusterCapabilities>):ClusterCapabilities=>({manualReassignmentAllowed:false,rebalancingStatus:'ACTIVE',reason:'',kind:'MSK Express',brokerType:'',observedAt:'2026-10-07T00:00:00Z',...over});
  it('lets a cluster that may run plans plan and run',()=>{
    expect(reassignmentAccess(caps({manualReassignmentAllowed:true,planningAllowed:true}))).toEqual({canPlan:true,canRun:true,planningOnly:false});
  });
  it('offers planning but not running while intelligent rebalancing is active',()=>{
    expect(reassignmentAccess(caps({planningAllowed:true}))).toEqual({canPlan:true,canRun:false,planningOnly:true});
  });
  it('offers nothing when the cluster cannot be verified',()=>{
    expect(reassignmentAccess(caps({planningAllowed:false}))).toEqual({canPlan:false,canRun:false,planningOnly:false});
    expect(reassignmentAccess(undefined)).toEqual({canPlan:false,canRun:false,planningOnly:false});
  });
  it('treats a backend that does not report planningAllowed like before',()=>{
    expect(reassignmentAccess(caps({manualReassignmentAllowed:true}))).toMatchObject({canPlan:true,canRun:true});
    expect(reassignmentAccess(caps({}))).toMatchObject({canPlan:false,canRun:false});
  });
  it('never lets running exceed planning',()=>{
    for(const manual of [true,false])for(const planning of [true,false,undefined]){
      const a=reassignmentAccess(caps({manualReassignmentAllowed:manual,planningAllowed:planning}));
      expect(a.canRun&&!a.canPlan).toBe(false);
    }
  });
});

describe('the active rebalancing notice',()=>{
  it('tells the user planning works and running does not',()=>{
    render(<CapabilityNotice capabilities={{manualReassignmentAllowed:false,planningAllowed:true,rebalancingStatus:'ACTIVE',reason:'r',kind:'MSK Express',brokerType:'',observedAt:'2026-10-07T00:00:00Z'}}/>);
    expect(screen.getByText(/generate and validate plans here/i)).toBeVisible();
    expect(screen.getByText(/Running one stays disabled/)).toBeVisible();
  });
});
