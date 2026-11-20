import {fireEvent,render,screen} from '@testing-library/react';
import {useState} from 'react';
import {describe,expect,it} from 'vitest';
import {NameFilter} from './NameFilter';
import type {Op} from './filter-query';

function Harness({text=''}:{text?:string}){const [op,setOp]=useState<Op>('starts');const [t,setT]=useState(text);return <NameFilter op={op} onOp={setOp} text={t} onText={setT}/>}

describe('NameFilter',()=>{
  it('offers the four match types and no regex',()=>{
    render(<Harness/>);
    const options=Array.from(screen.getByLabelText('Match type').querySelectorAll('option')).map(o=>o.textContent);
    expect(options).toEqual(['starts with','ends with','contains','equals']);
  });
  it('shows OR and NOT as colored chips and reads bare words with the menu choice',()=>{
    render(<Harness text="tmp test NOT keep"/>);
    const chips=screen.getByLabelText('How the filter is read');
    expect(chips.querySelector('.op-or')).not.toBeNull();
    expect(chips.querySelector('.op-not')).not.toBeNull();
    expect(chips).toHaveTextContent('Bare words use “starts with”.');
    fireEvent.change(screen.getByLabelText('Match type'),{target:{value:'contains'}});
    expect(chips).toHaveTextContent('contains tmp');
    expect(chips).toHaveTextContent('Bare words use “contains”.');
  });
  it('explains a filter that only keeps topics',()=>{
    render(<Harness text="NOT keep"/>);
    expect(screen.getByLabelText('How the filter is read')).toHaveTextContent(/NOT only keeps topics/);
  });
  it('flags problems on their own chip',()=>{
    render(<Harness text="a*b"/>);
    expect(screen.getByRole('alert')).toHaveTextContent(/Use \* only at the start or end/);
  });
  it('shows nothing under an empty bar',()=>{
    render(<Harness/>);
    expect(screen.queryByLabelText('How the filter is read')).toBeNull();
  });
});
