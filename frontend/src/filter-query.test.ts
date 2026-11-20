import {describe,expect,it} from 'vitest';
import {filterParam,parseFilter} from './filter-query';

const read=(text:string,op:'starts'|'ends'|'contains'|'equals'='starts')=>parseFilter(text,op);

describe('parseFilter',()=>{
  it('uses the menu match type for bare words and treats space and OR alike',()=>{
    expect(read('tmp test').include).toEqual([{op:'starts',value:'tmp'},{op:'starts',value:'test'}]);
    expect(read('tmp OR test').include).toEqual(read('tmp test').include);
    expect(read('scratch','contains').include).toEqual([{op:'contains',value:'scratch'}]);
  });
  it('lets * at either end override the menu',()=>{
    expect(read('tmp*','ends').include).toEqual([{op:'starts',value:'tmp'}]);
    expect(read('*-dlq').include).toEqual([{op:'ends',value:'-dlq'}]);
    expect(read('*keep*').include).toEqual([{op:'contains',value:'keep'}]);
  });
  it('accepts a match type prefix',()=>{
    expect(read('equals:orders').include).toEqual([{op:'equals',value:'orders'}]);
    expect(read('ends:-dlq','starts').include).toEqual([{op:'ends',value:'-dlq'}]);
  });
  it('sends NOT and a leading - to the except list',()=>{
    const p=read('tmp test NOT keep -*-prod');
    expect(p.include).toHaveLength(2);
    expect(p.except).toEqual([{op:'starts',value:'keep'},{op:'ends',value:'-prod'}]);
    expect(p.chips.filter(c=>c.not&&c.term)).toHaveLength(2);
  });
  it('keeps unusable words as problems and does not send them',()=>{
    const p=read('a*b AND * equals:x* NOT');
    expect(p.include).toEqual([]);
    expect(p.chips.map(c=>c.problem).filter(Boolean)).toHaveLength(5);
    expect(filterParam(p)).toBe('');
  });
  it('stops at 20 words per list',()=>{
    const p=read(Array.from({length:22},(_,i)=>`t${i}`).join(' '));
    expect(p.include).toHaveLength(20);
    expect(p.chips.filter(c=>c.problem)).toHaveLength(2);
  });
});

describe('filterParam',()=>{
  it('is empty with no words and JSON otherwise',()=>{
    expect(filterParam(read(''))).toBe('');
    expect(JSON.parse(filterParam(read('tmp NOT keep')))).toEqual({include:[{op:'starts',value:'tmp'}],except:[{op:'starts',value:'keep'}]});
  });
});
