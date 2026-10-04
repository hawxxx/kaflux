import {render,screen,fireEvent,waitFor} from '@testing-library/react';
import {afterEach,expect,it,vi} from 'vitest';
import {ThrottleControl} from './ThrottleControl';

afterEach(()=>{vi.restoreAllMocks();vi.unstubAllGlobals()});
const plan={id:'job-1',state:'running',planHash:'reviewed',topics:['orders.created'],throttleBytesPerSec:100};
it('requires exact job confirmation and separates requested from applied rate',async()=>{
  const fetch=vi.fn(async(_input:RequestInfo|URL,_options?:RequestInit)=>({ok:true,json:async()=>({data:{...plan,throttleRequest:{revision:'next',bytesPerSec:200}}})} as Response));vi.stubGlobal('fetch',fetch);
  render(<ThrottleControl clusterId="demo" plan={plan} canWrite allowed onChange={()=>{}}/>);
  fireEvent.click(screen.getByRole('button',{name:'Change throttle'}));
  fireEvent.change(screen.getByLabelText('New rate · bytes per second'),{target:{value:'200'}});
  const submit=screen.getByRole('button',{name:'Request throttle change'});expect(submit).toBeDisabled();
  fireEvent.change(screen.getByLabelText('Confirm job ID'),{target:{value:'job-1'}});
  fireEvent.click(submit);
  await waitFor(()=>expect(fetch).toHaveBeenCalledOnce());
  expect(JSON.parse(fetch.mock.calls[0][1]?.body as string)).toEqual({confirmation:true,planHash:'reviewed',bytesPerSec:200});
  await waitFor(()=>expect(screen.getByText('Requested: 200 B/s — awaiting worker verification')).toBeVisible());
  expect(screen.getByText('Current job throttle: 100 B/s')).toBeVisible();
});
it('disables changes while cancellation, cleanup, or another rate request is pending',()=>{
  const {rerender}=render(<ThrottleControl clusterId="demo" plan={{...plan,cancellationRequested:true}} canWrite allowed onChange={()=>{}}/>);
  expect(screen.getByRole('button',{name:'Change throttle'})).toBeDisabled();
  rerender(<ThrottleControl clusterId="demo" plan={{...plan,cleanupPending:true}} canWrite allowed onChange={()=>{}}/>);
  expect(screen.getByRole('button',{name:'Change throttle'})).toBeDisabled();
  rerender(<ThrottleControl clusterId="demo" plan={{...plan,throttleRequest:{revision:'pending',bytesPerSec:300}}} canWrite allowed onChange={()=>{}}/>);
  expect(screen.getByRole('button',{name:'Change throttle'})).toBeDisabled();
});
it('shows backend denial without changing the current rate',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>({ok:false,status:403,json:async()=>({error:{code:'forbidden',message:'Permission denied'}})} as Response)));
  render(<ThrottleControl clusterId="demo" plan={plan} canWrite allowed onChange={()=>{}}/>);
  fireEvent.click(screen.getByRole('button',{name:'Change throttle'}));
  fireEvent.change(screen.getByLabelText('New rate · bytes per second'),{target:{value:'200'}});
  fireEvent.change(screen.getByLabelText('Confirm job ID'),{target:{value:'job-1'}});
  fireEvent.click(screen.getByRole('button',{name:'Request throttle change'}));
  await waitFor(()=>expect(screen.getByRole('alert')).toHaveTextContent('Permission denied'));
  expect(screen.getByText('Current job throttle: 100 B/s')).toBeVisible();
});
