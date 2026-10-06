import {fireEvent,render,screen,within} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {compareValues,SortTh,useSortedRows} from './table-sort';
import {validateTopicSearch} from './topic-search';

const rows=[{name:'broker-10',size:300},{name:'broker-2',size:null},{name:'broker-1',size:50}];
function Table(){const table=useSortedRows(rows,{name:r=>r.name,size:r=>r.size});return <table><thead><tr><SortTh label="Name" sortKey="name" table={table}/><SortTh label="Size" sortKey="size" table={table}/></tr></thead><tbody>{table.sorted.map(r=><tr key={r.name}><td>{r.name}</td></tr>)}</tbody></table>}
const order=()=>within(screen.getAllByRole('rowgroup')[1]).getAllByRole('cell').map(c=>c.textContent);

describe('table sorting',()=>{
  it('compares numbers numerically, text naturally and missing values lowest',()=>{
    expect(compareValues(9,10)).toBeLessThan(0);
    expect(compareValues('p2','p10')).toBeLessThan(0);
    expect(compareValues(null,0)).toBeLessThan(0);
    expect(compareValues(undefined,null)).toBe(0);
  });
  it('cycles ascending, descending and source order on header clicks',()=>{
    render(<Table/>);
    const size=screen.getByRole('button',{name:'Size'});
    fireEvent.click(size);
    expect(order()).toEqual(['broker-2','broker-1','broker-10']);
    expect(screen.getByRole('columnheader',{name:'Size'})).toHaveAttribute('aria-sort','ascending');
    fireEvent.click(size);
    expect(order()).toEqual(['broker-10','broker-1','broker-2']);
    fireEvent.click(size);
    expect(order()).toEqual(['broker-10','broker-2','broker-1']);
    expect(screen.getByRole('columnheader',{name:'Size'})).toHaveAttribute('aria-sort','none');
  });
  it('accepts only known topic sort columns from the URL',()=>{
    expect(validateTopicSearch({sort:'sizeBytes'}).sort).toBe('sizeBytes');
    expect(validateTopicSearch({sort:'retentionMs'}).sort).toBe('name');
  });
});
