import {fireEvent,render,screen} from '@testing-library/react';
import {useState} from 'react';
import {expect,it} from 'vitest';
import {NumberStepper,clampStep} from './NumberStepper';

function Harness({initial='5',min=1,max=10,step=1}:{initial?:string;min?:number;max?:number;step?:number}){
  const [value,setValue]=useState(initial);
  return <><label htmlFor="days">Days</label><NumberStepper id="days" label="days" value={value} onChange={setValue} min={min} max={max} step={step}/><output data-testid="value">{value}</output></>;
}

it('exposes an accessible spinbutton without a native number input',()=>{
  const {container}=render(<Harness/>);
  const input=screen.getByRole('spinbutton',{name:'Days'});
  expect(input).toHaveAttribute('type','text');
  expect(input).toHaveAttribute('inputmode','numeric');
  expect(input).toHaveAttribute('aria-valuenow','5');
  expect(input).toHaveAttribute('aria-valuemin','1');
  expect(input).toHaveAttribute('aria-valuemax','10');
  expect(container.querySelector('input[type=number]')).toBeNull();
});

it('steps with the buttons and stops at the bounds',()=>{
  render(<Harness initial="9"/>);
  const up=screen.getByRole('button',{name:'Increase days'});
  const down=screen.getByRole('button',{name:'Decrease days'});
  fireEvent.click(up);
  expect(screen.getByTestId('value')).toHaveTextContent('10');
  expect(up).toBeDisabled();
  fireEvent.click(up);
  expect(screen.getByTestId('value')).toHaveTextContent('10');
  fireEvent.click(down);
  expect(screen.getByTestId('value')).toHaveTextContent('9');
});

it('supports arrow, page, home and end keys',()=>{
  render(<Harness initial="5" max={90}/>);
  const input=screen.getByRole('spinbutton',{name:'Days'});
  fireEvent.keyDown(input,{key:'ArrowUp'});
  expect(screen.getByTestId('value')).toHaveTextContent('6');
  fireEvent.keyDown(input,{key:'ArrowDown'});
  fireEvent.keyDown(input,{key:'ArrowDown'});
  expect(screen.getByTestId('value')).toHaveTextContent('4');
  fireEvent.keyDown(input,{key:'PageUp'});
  expect(screen.getByTestId('value')).toHaveTextContent('14');
  fireEvent.keyDown(input,{key:'End'});
  expect(screen.getByTestId('value')).toHaveTextContent('90');
  fireEvent.keyDown(input,{key:'Home'});
  expect(screen.getByTestId('value')).toHaveTextContent('1');
});

it('accepts typed digits, ignores other characters and leaves out-of-range values for validation',()=>{
  render(<Harness/>);
  const input=screen.getByRole('spinbutton',{name:'Days'});
  fireEvent.change(input,{target:{value:'12'}});
  expect(screen.getByTestId('value')).toHaveTextContent('12');
  expect(input).not.toHaveAttribute('aria-valuenow');
  fireEvent.change(input,{target:{value:'1x'}});
  expect(screen.getByTestId('value')).toHaveTextContent('12');
  fireEvent.change(input,{target:{value:''}});
  expect(screen.getByTestId('value')).toHaveTextContent('');
  fireEvent.click(screen.getByRole('button',{name:'Increase days'}));
  expect(screen.getByTestId('value')).toHaveTextContent('1');
});

it('clamps stepped values to the range and step',()=>{
  expect(clampStep('',1,1,10,1)).toBe(1);
  expect(clampStep('50',1,1,10,1)).toBe(10);
  expect(clampStep('0',-1,1,10,1)).toBe(1);
  expect(clampStep('4',1,0,100,5)).toBe(9);
});

it('disables both buttons when disabled',()=>{
  render(<NumberStepper label="days" value="3" onChange={()=>{}} min={1} max={5} disabled/>);
  expect(screen.getByRole('button',{name:'Increase days'})).toBeDisabled();
  expect(screen.getByRole('button',{name:'Decrease days'})).toBeDisabled();
  expect(screen.getByRole('spinbutton')).toBeDisabled();
});
