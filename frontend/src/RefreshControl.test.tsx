import {act,fireEvent,render,screen} from '@testing-library/react';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {RefreshControl,loadInterval} from './RefreshControl';

let visibility:DocumentVisibilityState='visible';
function memoryStorage():Storage{const m=new Map<string,string>();return {get length(){return m.size},clear:()=>m.clear(),getItem:k=>m.get(k)??null,key:i=>[...m.keys()][i]??null,removeItem:k=>{m.delete(k)},setItem:(k,v)=>{m.set(k,String(v))}}}
beforeEach(()=>{
  vi.stubGlobal('localStorage',memoryStorage());
  vi.useFakeTimers();visibility='visible';
  vi.spyOn(document,'visibilityState','get').mockImplementation(()=>visibility);
});
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();vi.unstubAllGlobals()});
const flush=async()=>{await act(async()=>{await Promise.resolve()})};
const advance=async(ms:number)=>{await act(async()=>{await vi.advanceTimersByTimeAsync(ms)})};

describe('loadInterval',()=>{
  it('accepts only the offered values',()=>{
    expect(loadInterval({getItem:()=>'10000'})).toBe(10000);
    expect(loadInterval({getItem:()=>'1'})).toBe(0);
    expect(loadInterval({getItem:()=>'abc'})).toBe(0);
    expect(loadInterval({getItem:()=>null})).toBe(0);
    expect(loadInterval({getItem:()=>{throw new Error('blocked')}})).toBe(0);
  });
});

describe('RefreshControl',()=>{
  it('is off by default and refreshes only when clicked',async()=>{
    const onRefresh=vi.fn(async()=>{});
    render(<RefreshControl onRefresh={onRefresh}/>);
    expect(screen.getByLabelText('Auto refresh interval')).toHaveValue('0');
    await advance(120_000);
    expect(onRefresh).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button',{name:'Refresh'}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
  it('refreshes on the chosen interval and remembers it',async()=>{
    const onRefresh=vi.fn(async()=>{});
    const {unmount}=render(<RefreshControl onRefresh={onRefresh}/>);
    fireEvent.change(screen.getByLabelText('Auto refresh interval'),{target:{value:'5000'}});
    expect(localStorage.getItem('kaflux-refresh-interval')).toBe('5000');
    await advance(4_900);expect(onRefresh).toHaveBeenCalledTimes(0);
    await advance(200);expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(2);
    unmount();
    render(<RefreshControl onRefresh={onRefresh}/>);
    expect(screen.getByLabelText('Auto refresh interval')).toHaveValue('5000');
    expect(screen.getByText('5s')).toBeInTheDocument();
  });
  it('never starts a refresh while the previous one is still running',async()=>{
    let release:()=>void=()=>{};
    const onRefresh=vi.fn(()=>new Promise<void>(r=>{release=r}));
    localStorage.setItem('kaflux-refresh-interval','5000');
    render(<RefreshControl onRefresh={onRefresh}/>);
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(30_000);expect(onRefresh).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button',{name:/Refresh/}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await act(async()=>{release()});
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(2);
  });
  it('pauses while the tab is hidden and catches up when it is shown again',async()=>{
    const onRefresh=vi.fn(async()=>{});
    localStorage.setItem('kaflux-refresh-interval','10000');
    render(<RefreshControl onRefresh={onRefresh}/>);
    visibility='hidden';
    await advance(60_000);expect(onRefresh).not.toHaveBeenCalled();
    visibility='visible';
    await act(async()=>{document.dispatchEvent(new Event('visibilitychange'))});
    await advance(0);expect(onRefresh).toHaveBeenCalledTimes(1);
  });
  it('stops when switched off and leaves no timers after unmount',async()=>{
    const onRefresh=vi.fn(async()=>{});
    const {unmount}=render(<RefreshControl onRefresh={onRefresh}/>);
    const select=screen.getByLabelText('Auto refresh interval');
    fireEvent.change(select,{target:{value:'5000'}});await advance(5_000);
    fireEvent.change(select,{target:{value:'0'}});await advance(60_000);
    expect(onRefresh).toHaveBeenCalledTimes(1);
    fireEvent.change(select,{target:{value:'10000'}});unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
  it('keeps refreshing after a failed refresh',async()=>{
    const onRefresh=vi.fn().mockRejectedValueOnce(new Error('broker unavailable')).mockResolvedValue(undefined);
    localStorage.setItem('kaflux-refresh-interval','5000');
    render(<RefreshControl onRefresh={onRefresh}/>);
    await advance(5_000);await advance(5_000);
    expect(onRefresh).toHaveBeenCalledTimes(2);
  });
  it('works when the browser blocks storage',async()=>{
    vi.stubGlobal('localStorage',{getItem:()=>{throw new DOMException('blocked','SecurityError')},setItem:()=>{throw new DOMException('blocked','SecurityError')}});
    const onRefresh=vi.fn(async()=>{});
    render(<RefreshControl onRefresh={onRefresh}/>);
    fireEvent.change(screen.getByLabelText('Auto refresh interval'),{target:{value:'5000'}});
    await advance(5_000);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
  it('skips a timed refresh that lands while a manual refresh is still running',async()=>{
    let release:()=>void=()=>{};
    const onRefresh=vi.fn(()=>new Promise<void>(r=>{release=r}));
    localStorage.setItem('kaflux-refresh-interval','5000');
    render(<RefreshControl onRefresh={onRefresh}/>);
    await advance(4_000);
    fireEvent.click(screen.getByRole('button',{name:'Refresh'}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(1_500);
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await act(async()=>{release()});
  });
  it('keeps the visible label inside the button so narrow layouts can hide it without losing the name',()=>{
    render(<RefreshControl onRefresh={vi.fn(async()=>{})}/>);
    const button=screen.getByRole('button',{name:'Refresh'});
    expect(button.querySelector('.refresh-label')).toHaveTextContent('Refresh');
    expect(button.querySelector('svg')).toHaveAttribute('aria-hidden','true');
  });
});
