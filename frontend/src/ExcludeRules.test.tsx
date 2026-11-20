import {fireEvent,render,screen} from '@testing-library/react';
import {useState} from 'react';
import {describe,expect,it} from 'vitest';
import {ExcludeRules,rulesParam,ruleProblem,type Rule} from './ExcludeRules';

const rule=(over:Partial<Rule>={}):Rule=>({id:1,op:'starts',value:'tmp',not:false,join:'or',...over});
function Harness({initial=[]}:{initial?:Rule[]}){const [rules,setRules]=useState<Rule[]>(initial);return <><ExcludeRules rules={rules} onChange={setRules}/><output data-testid="param">{rulesParam(rules)}</output></>}

describe('rulesParam',()=>{
  it('is empty until a rule has a value',()=>{
    expect(rulesParam([])).toBe('');
    expect(rulesParam([rule({value:'  '})])).toBe('');
  });
  it('sends complete rules only, trimmed, with their NOT and join',()=>{
    const json=rulesParam([rule({value:' tmp '}),rule({id:2,op:'contains',value:'keep',not:true,join:'and'}),rule({id:3,value:''})]);
    expect(JSON.parse(json)).toEqual([{op:'starts',value:'tmp',not:false,join:'or'},{op:'contains',value:'keep',not:true,join:'and'}]);
  });
  it('leaves out a rule whose regex does not compile',()=>{
    expect(ruleProblem(rule({op:'regex',value:'('}))).toMatch(/valid regular expression/);
    expect(rulesParam([rule({op:'regex',value:'('})])).toBe('');
    expect(rulesParam([rule({op:'regex',value:'^a'})])).not.toBe('');
  });
});

describe('ExcludeRules',()=>{
  it('adds rules and shows the operators in a summary',()=>{
    render(<Harness/>);
    expect(screen.getByText(/No name rules/)).toBeVisible();
    fireEvent.click(screen.getByRole('button',{name:/Add rule/}));
    fireEvent.change(screen.getByLabelText('Value for rule 1'),{target:{value:'tmp'}});
    fireEvent.click(screen.getByRole('button',{name:/Add rule/}));
    fireEvent.change(screen.getByLabelText('Value for rule 2'),{target:{value:'keep'}});
    fireEvent.change(screen.getByLabelText('Match type for rule 2'),{target:{value:'contains'}});
    fireEvent.click(screen.getByRole('button',{name:'Negate rule 2'}));
    fireEvent.click(screen.getByRole('button',{name:/Rule 2 joins the previous rule with OR/}));
    const summary=screen.getByLabelText('Rule summary');
    expect(summary).toHaveTextContent('starts with tmp AND NOT contains keep');
    expect(summary.querySelector('.op-and')).not.toBeNull();
    expect(summary.querySelector('.op-not')).not.toBeNull();
    expect(JSON.parse(screen.getByTestId('param').textContent!)).toEqual([{op:'starts',value:'tmp',not:false,join:'or'},{op:'contains',value:'keep',not:true,join:'and'}]);
  });
  it('removes a rule and flags a bad regex without sending it',()=>{
    render(<Harness initial={[rule(),rule({id:2,op:'regex',value:'(',join:'or'})]}/>);
    expect(screen.getByRole('alert')).toHaveTextContent(/valid regular expression/);
    expect(JSON.parse(screen.getByTestId('param').textContent!)).toHaveLength(1);
    fireEvent.click(screen.getByRole('button',{name:'Remove rule 2'}));
    expect(screen.queryByRole('alert')).toBeNull();
    expect(screen.queryByLabelText('Value for rule 2')).toBeNull();
  });
  it('offers no join before the first rule and toggles the NOT state for assistive tech',()=>{
    render(<Harness initial={[rule()]}/>);
    expect(screen.queryByRole('button',{name:/joins the previous rule/})).toBeNull();
    const not=screen.getByRole('button',{name:'Negate rule 1'});
    expect(not).toHaveAttribute('aria-pressed','false');
    fireEvent.click(not);
    expect(not).toHaveAttribute('aria-pressed','true');
  });
});
