import {describe,expect,it} from 'vitest';
import {awaitingClusterList} from './clusterRoute';

describe('awaitingClusterList',()=>{
  it('waits while the list is loading and the URL has no cluster',()=>{expect(awaitingClusterList(false,false,false)).toBe(true)});
  it('does not wait once the list has loaded',()=>{expect(awaitingClusterList(false,true,false)).toBe(false)});
  it('does not wait when the list failed, so the error shows',()=>{expect(awaitingClusterList(false,false,true)).toBe(false)});
  it('does not wait when the URL already names a cluster',()=>{expect(awaitingClusterList(true,false,false)).toBe(false)});
});
