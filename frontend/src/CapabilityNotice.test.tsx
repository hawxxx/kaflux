import {render,screen} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {CapabilityNotice} from './CapabilityNotice';
describe('cluster capability preflight',()=>{
  it('explains AWS ownership while intelligent balancing is active',()=>{render(<CapabilityNotice capabilities={{manualReassignmentAllowed:false,rebalancingStatus:'ACTIVE',reason:'MSK intelligent balancing is active.',kind:'msk',brokerType:'express',observedAt:'2026-10-03T00:00:00Z'}}/>);expect(screen.getByText('AWS owns partition balancing')).toBeVisible();expect(screen.getByText(/Pause intelligent rebalancing through AWS MSK/)).toBeVisible()});
  it('does not imply permission when capability is unknown',()=>{render(<CapabilityNotice/>);expect(screen.getByText(/disabled until capability is known/)).toBeVisible()});
});
