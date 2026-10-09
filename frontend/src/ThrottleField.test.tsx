import {fireEvent,render,screen} from '@testing-library/react';
import {expect,it,vi} from 'vitest';
import {ThrottleField,throttleRate} from './ThrottleField';

it('turns the throttle off and sends zero',()=>{
  const onEnabled=vi.fn();
  const {rerender}=render(<ThrottleField enabled required={false} value="10485760" onEnabled={onEnabled} onValue={()=>{}}/>);
  expect(screen.getByText('10.0 MiB/s per broker')).toBeVisible();
  fireEvent.click(screen.getByRole('switch',{name:'Replication throttle'}));
  expect(onEnabled).toHaveBeenCalledWith(false);
  rerender(<ThrottleField enabled={false} required={false} value="10485760" onEnabled={onEnabled} onValue={()=>{}}/>);
  expect(screen.getByLabelText('Throttle bytes per second')).toBeDisabled();
  expect(screen.getByText('Unthrottled: moves run at full speed')).toBeVisible();
  expect(throttleRate(false,false,'10485760')).toBe(0);
  expect(throttleRate(true,false,'10485760')).toBe(10485760);
});

it('locks the throttle on when the cluster requires it',()=>{
  render(<ThrottleField enabled={false} required value="10485760" onEnabled={()=>{}} onValue={()=>{}}/>);
  expect(screen.getByRole('switch',{name:'Replication throttle'})).toBeChecked();
  expect(screen.getByRole('switch',{name:'Replication throttle'})).toBeDisabled();
  expect(screen.getByText('Required on this cluster')).toBeVisible();
  expect(throttleRate(false,true,'5')).toBe(5);
});
