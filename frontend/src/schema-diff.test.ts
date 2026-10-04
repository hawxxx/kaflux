import {expect,it} from 'vitest';
import {schemaDiff} from './schema-diff';
it('preserves repeated schema lines and marks an inserted field',()=>{
  expect(schemaDiff('a\nx\na','a\ny\nx\na')).toEqual([{kind:'same',line:'a'},{kind:'added',line:'y'},{kind:'same',line:'x'},{kind:'same',line:'a'}]);
});
it('marks replacement and refuses an unbounded comparison',()=>{
  expect(schemaDiff('old','new')).toEqual([{kind:'removed',line:'old'},{kind:'added',line:'new'}]);
  expect(()=>schemaDiff('x\n'.repeat(601),'')).toThrow('600 lines');
});
