import {fireEvent,render,screen} from '@testing-library/react';
import {useState} from 'react';
import {expect,it} from 'vitest';
import {Picker} from './Picker';
const names=['cleanup.policy','compression.type','retention.ms','retention.bytes','segment.ms','segment.bytes','min.insync.replicas','max.message.bytes'];
function Harness(){const [v,setV]=useState('');return <><Picker value={v} onChange={setV} label="Setting" placeholder="Pick…" options={names.map(value=>({value}))}/><output>{v}</output></>}
it('filters options as you type and picks with the keyboard',()=>{
  render(<Harness/>);
  fireEvent.click(screen.getByRole('combobox',{name:'Setting'}));
  expect(screen.getAllByRole('option')).toHaveLength(8);
  const search=screen.getByLabelText('Search Setting');
  fireEvent.change(search,{target:{value:'RETENTION'}});
  expect(screen.getAllByRole('option').map(o=>o.textContent)).toEqual(['retention.ms','retention.bytes']);
  fireEvent.keyDown(search,{key:'ArrowDown'});
  fireEvent.keyDown(search,{key:'Enter'});
  expect(screen.getByRole('status')).toHaveTextContent('retention.bytes');
  expect(screen.queryByRole('listbox')).toBeNull();
});
it('shows an empty state when nothing matches',()=>{
  render(<Harness/>);
  fireEvent.click(screen.getByRole('combobox',{name:'Setting'}));
  fireEvent.change(screen.getByLabelText('Search Setting'),{target:{value:'zzz'}});
  expect(screen.queryAllByRole('option')).toHaveLength(0);
  expect(screen.getByText('No matches')).toBeTruthy();
});
