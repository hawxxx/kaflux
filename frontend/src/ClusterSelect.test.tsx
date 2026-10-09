import {fireEvent,render,screen} from '@testing-library/react';
import {describe,expect,it,vi} from 'vitest';
import {ClusterSelect} from './ClusterSelect';
import type {Cluster} from './api';

const clusters=[{id:'a',name:'Alpha',environment:'prod',mode:'Kafka'},{id:'b',name:'Beta',environment:'dev',mode:'Kafka'}] as unknown as Cluster[];

describe('ClusterSelect',()=>{
  it('shows the current cluster and switches with the keyboard',()=>{
    const onChange=vi.fn();
    render(<ClusterSelect clusters={clusters} value="a" onChange={onChange}/>);
    const trigger=screen.getByRole('button',{name:'Select cluster: Alpha'});
    fireEvent.keyDown(trigger,{key:'ArrowDown'});
    const list=screen.getByRole('listbox');
    fireEvent.keyDown(list,{key:'ArrowDown'});
    fireEvent.keyDown(list,{key:'Enter'});
    expect(onChange).toHaveBeenCalledWith('b');
    expect(screen.queryByRole('listbox')).toBeNull();
  });
  it('does not navigate when the current cluster is chosen',()=>{
    const onChange=vi.fn();
    render(<ClusterSelect clusters={clusters} value="a" onChange={onChange}/>);
    fireEvent.click(screen.getByRole('button',{name:/Select cluster/}));
    fireEvent.click(screen.getByRole('option',{name:/Alpha/}));
    expect(onChange).not.toHaveBeenCalled();
  });
});
