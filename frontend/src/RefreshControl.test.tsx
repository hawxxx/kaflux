import {act,fireEvent,render,screen} from '@testing-library/react';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {RefreshControl,RefreshProvider,formatInterval,loadInterval,useEssentialInterval,useSteadyInterval} from './RefreshControl';

let visibility:DocumentVisibilityState='visible';
function memoryStorage():Storage{const m=new Map<string,string>();return {get length(){return m.size},clear:()=>m.clear(),getItem:k=>m.get(k)??null,key:i=>[...m.keys()][i]??null,removeItem:k=>{m.delete(k)},setItem:(k,v)=>{m.set(k,String(v))}}}
beforeEach(()=>{
  vi.stubGlobal('localStorage',memoryStorage());
  // Only the timer APIs the component uses; React's own scheduler work (setImmediate) stays real.
  vi.useFakeTimers({toFake:['setTimeout','clearTimeout','setInterval','clearInterval','Date']});visibility='visible';
  vi.spyOn(document,'visibilityState','get').mockImplementation(()=>visibility);
});
afterEach(()=>{vi.useRealTimers();vi.restoreAllMocks();vi.unstubAllGlobals()});
const trigger=()=>screen.getByRole('button',{name:/^Auto refresh:/});
const pick=(name:string)=>{fireEvent.click(trigger());fireEvent.click(screen.getByRole('option',{name:new RegExp(`^${name}`)}))};
const flush=async()=>{await act(async()=>{await Promise.resolve()})};
const advance=async(ms:number)=>{await act(async()=>{await vi.advanceTimersByTimeAsync(ms)})};
const mount=(onRefresh=vi.fn(async()=>{}),extra:React.ReactNode=null)=>({onRefresh,...render(<RefreshProvider><RefreshControl onRefresh={onRefresh}/>{extra}</RefreshProvider>)});

/** A stand-in for a page that refreshes itself, the way Overview does every 30 seconds. */
function SteadyPage({ms=30_000,onInterval}:{ms?:number;onInterval?:(v:number|false)=>void}){const v=useSteadyInterval(ms);onInterval?.(v);return <p>page</p>}
function LivePage({ms=3_000}:{ms?:number}){useEssentialInterval(ms);return <p>live</p>}

describe('formatInterval',()=>{
  it('writes seconds and minutes',()=>{
    expect([0,5_000,30_000,60_000,120_000,300_000,45_000].map(formatInterval)).toEqual(['Off','5s','30s','1m','2m','5m','45s']);
  });
});

describe('naming intervals that are not menu choices',()=>{
  it('spells the unit out so a live 3 second update reads like the others',async()=>{
    mount(undefined,<LivePage ms={3_000}/>);await flush();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 3 seconds, page default');
    mount(undefined,<LivePage ms={120_000}/>);await flush();
  });
});

describe('loadInterval',()=>{
  it('returns the offered value that was saved',()=>{expect(loadInterval({getItem:()=>'10000'})).toBe(10000)});
  it('keeps a deliberate Off apart from no choice',()=>{
    expect(loadInterval({getItem:()=>'0'})).toBe(0);
    expect(loadInterval({getItem:()=>null})).toBeNull();
    expect(loadInterval({getItem:()=>''})).toBeNull();
  });
  it('ignores anything that was not offered, and blocked storage',()=>{
    expect(loadInterval({getItem:()=>'1'})).toBeNull();
    expect(loadInterval({getItem:()=>'abc'})).toBeNull();
    expect(loadInterval({getItem:()=>{throw new Error('blocked')}})).toBeNull();
  });
});

describe('what the label says',()=>{
  it('reads Off on a page that never refreshes itself, and says it is the default',()=>{
    mount();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Off, page default');
    expect(trigger()).toHaveTextContent('Off');
  });
  it('shows the interval a page really refreshes at, not Off',async()=>{
    mount(undefined,<SteadyPage ms={30_000}/>);
    await flush();
    expect(trigger()).toHaveTextContent('30s');
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 30 seconds, page default');
    expect(trigger()).toHaveAttribute('data-active','true');
  });
  it('shows the shortest default when several things refresh on one page',async()=>{
    mount(undefined,<><SteadyPage ms={30_000}/><SteadyPage ms={10_000}/></>);
    await flush();
    expect(trigger()).toHaveTextContent('10s');
  });
  it('follows the page when it changes, and goes back to Off when nothing refreshes',async()=>{
    const {rerender,onRefresh}=mount(undefined,<SteadyPage ms={30_000}/>);
    await flush();expect(trigger()).toHaveTextContent('30s');
    rerender(<RefreshProvider><RefreshControl onRefresh={onRefresh}/></RefreshProvider>);
    await flush();expect(trigger()).toHaveTextContent('Off');
  });
  it('labels the first menu entry with the current page default',async()=>{
    mount(undefined,<SteadyPage ms={30_000}/>);await flush();
    fireEvent.click(trigger());
    expect(screen.getAllByRole('option')).toHaveLength(7);
    const first=screen.getAllByRole('option')[0];
    expect(first).toHaveTextContent('Page default');
    expect(first).toHaveTextContent('30s');
    expect(first).toHaveAttribute('aria-selected','true');
  });
});

describe('choosing an interval',()=>{
  it('hands the schedule to the picker: pages stop polling on their own',async()=>{
    const seen:Array<number|false>=[];
    mount(undefined,<SteadyPage ms={30_000} onInterval={v=>seen.push(v)}/>);await flush();
    expect(seen.at(-1)).toBe(30_000);
    pick('Every 5 seconds');
    expect(seen.at(-1)).toBe(false);
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 5 seconds');
  });
  it('Off really turns the page polling off as well',async()=>{
    const seen:Array<number|false>=[];
    const {onRefresh}=mount(undefined,<SteadyPage ms={30_000} onInterval={v=>seen.push(v)}/>);await flush();
    pick('Off');
    expect(seen.at(-1)).toBe(false);
    expect(trigger()).toHaveTextContent('Off');
    await advance(300_000);
    expect(onRefresh).not.toHaveBeenCalled();
  });
  it('choosing Page default again returns to what each page does',async()=>{
    const seen:Array<number|false>=[];
    mount(undefined,<SteadyPage ms={30_000} onInterval={v=>seen.push(v)}/>);await flush();
    pick('Every 5 seconds');pick('Page default');
    expect(seen.at(-1)).toBe(30_000);
    expect(localStorage.getItem('kaflux-refresh-interval')).toBeNull();
    expect(trigger()).toHaveTextContent('30s');
  });
  it('refreshes on the chosen interval and remembers it',async()=>{
    const {onRefresh,unmount}=mount();
    pick('Every 5 seconds');
    expect(localStorage.getItem('kaflux-refresh-interval')).toBe('5000');
    await advance(4_900);expect(onRefresh).toHaveBeenCalledTimes(0);
    await advance(200);expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(2);
    unmount();
    mount();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 5 seconds');
    expect(trigger()).toHaveTextContent('5s');
  });
  it('remembers a deliberate Off across a reload',()=>{
    mount();pick('Off');
    expect(localStorage.getItem('kaflux-refresh-interval')).toBe('0');
    cleanupAndRemount();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Off');
  });
});
function cleanupAndRemount(){document.body.innerHTML='';mount()}

describe('timing',()=>{
  it('is quiet by default and refreshes only when clicked',async()=>{
    const {onRefresh}=mount();
    await advance(120_000);
    expect(onRefresh).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button',{name:'Refresh'}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
  it('never starts a refresh while the previous one is still running',async()=>{
    let release:()=>void=()=>{};
    const onRefresh=vi.fn(()=>new Promise<void>(r=>{release=r}));
    localStorage.setItem('kaflux-refresh-interval','5000');
    mount(onRefresh);
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(30_000);expect(onRefresh).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button',{name:/Refresh/}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await act(async()=>{release()});
    await advance(5_000);expect(onRefresh).toHaveBeenCalledTimes(2);
  });
  it('skips a timed refresh that lands while a manual refresh is still running',async()=>{
    let release:()=>void=()=>{};
    const onRefresh=vi.fn(()=>new Promise<void>(r=>{release=r}));
    localStorage.setItem('kaflux-refresh-interval','5000');
    mount(onRefresh);
    await advance(4_000);
    fireEvent.click(screen.getByRole('button',{name:'Refresh'}));await flush();
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await advance(1_500);
    expect(onRefresh).toHaveBeenCalledTimes(1);
    await act(async()=>{release()});
  });
  it('pauses while the tab is hidden and catches up when it is shown again',async()=>{
    const onRefresh=vi.fn(async()=>{});
    localStorage.setItem('kaflux-refresh-interval','10000');
    mount(onRefresh);
    visibility='hidden';
    await advance(60_000);expect(onRefresh).not.toHaveBeenCalled();
    visibility='visible';
    await act(async()=>{document.dispatchEvent(new Event('visibilitychange'))});
    await advance(0);expect(onRefresh).toHaveBeenCalledTimes(1);
  });
  it('stops when switched off and stops refreshing after unmount',async()=>{
    const {onRefresh,unmount}=mount();
    pick('Every 5 seconds');await advance(5_000);
    pick('Off');await advance(60_000);
    expect(onRefresh).toHaveBeenCalledTimes(1);
    pick('Every 10 seconds');
    const calls=onRefresh.mock.calls.length;
    unmount();
    // jsdom schedules its own timers when focus moves, so count what the component does, not timers.
    await advance(120_000);
    expect(onRefresh).toHaveBeenCalledTimes(calls);
  });
  it('keeps refreshing after a failed refresh',async()=>{
    const onRefresh=vi.fn().mockRejectedValueOnce(new Error('broker unavailable')).mockResolvedValue(undefined);
    localStorage.setItem('kaflux-refresh-interval','5000');
    mount(onRefresh);
    await advance(5_000);await advance(5_000);
    expect(onRefresh).toHaveBeenCalledTimes(2);
  });
  it('works when the browser blocks storage',async()=>{
    vi.stubGlobal('localStorage',{getItem:()=>{throw new DOMException('blocked','SecurityError')},setItem:()=>{throw new DOMException('blocked','SecurityError')},removeItem:()=>{throw new DOMException('blocked','SecurityError')}});
    const {onRefresh}=mount();
    pick('Every 5 seconds');
    await advance(5_000);
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });
});

describe('what keeps updating whatever is chosen',()=>{
  it('says so in the menu, with its interval',async()=>{
    mount(undefined,<LivePage ms={3_000}/>);await flush();
    pick('Off');
    fireEvent.click(trigger());
    expect(screen.getByText(/Live job status and live tail keep updating every 3s/)).toBeInTheDocument();
  });
  it('counts live updates in the default label, so the label matches what the page does',async()=>{
    mount(undefined,<LivePage ms={3_000}/>);await flush();
    expect(trigger()).toHaveTextContent('3s');
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 3 seconds, page default');
  });
  it('shows the shortest of page refreshes and live updates',async()=>{
    mount(undefined,<><SteadyPage ms={30_000}/><LivePage ms={5_000}/></>);await flush();
    expect(trigger()).toHaveTextContent('5s');
  });
  it('after a choice the label is the choice, and the name still tells what keeps updating',async()=>{
    mount(undefined,<LivePage ms={3_000}/>);await flush();
    pick('Off');
    expect(trigger()).toHaveTextContent('Off');
    expect(trigger()).toHaveAccessibleName('Auto refresh: Off. Live job status and live tail still update every 3s');
  });
  it('stays silent in the menu when nothing like that is on screen',()=>{
    mount();fireEvent.click(trigger());
    expect(screen.queryByText(/keep updating/)).toBeNull();
  });
});

describe('structure and keyboard',()=>{
  it('keeps the visible label inside the button so narrow layouts can hide it without losing the name',()=>{
    mount();
    const button=screen.getByRole('button',{name:'Refresh'});
    expect(button.querySelector('.refresh-label')).toHaveTextContent('Refresh');
    expect(button.querySelector('svg')).toHaveAttribute('aria-hidden','true');
  });
  it('opens a listbox that marks the current choice, closes on Escape and returns focus',async()=>{
    mount();
    expect(screen.queryByRole('listbox')).toBeNull();
    fireEvent.click(trigger());
    expect(trigger()).toHaveAttribute('aria-expanded','true');
    const list=screen.getByRole('listbox',{name:'Auto refresh'});
    expect(screen.getAllByRole('option')).toHaveLength(7);
    expect(screen.getAllByRole('option').filter(o=>o.getAttribute('aria-selected')==='true')).toHaveLength(1);
    await flush();expect(list).toHaveFocus();
    fireEvent.keyDown(list,{key:'Escape'});
    expect(screen.queryByRole('listbox')).toBeNull();
    expect(trigger()).toHaveFocus();
  });
  it('is fully usable from the keyboard',async()=>{
    mount();
    fireEvent.keyDown(trigger(),{key:'ArrowDown'});
    const list=screen.getByRole('listbox');
    fireEvent.keyDown(list,{key:'ArrowDown'});fireEvent.keyDown(list,{key:'ArrowDown'});
    expect(list.getAttribute('aria-activedescendant')).toBe(screen.getByRole('option',{name:/^Every 5 seconds/}).id);
    fireEvent.keyDown(list,{key:'End'});fireEvent.keyDown(list,{key:'Home'});fireEvent.keyDown(list,{key:'ArrowDown'});fireEvent.keyDown(list,{key:'ArrowDown'});
    fireEvent.keyDown(list,{key:'Enter'});
    expect(screen.queryByRole('listbox')).toBeNull();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Every 5 seconds');
    expect(localStorage.getItem('kaflux-refresh-interval')).toBe('5000');
  });
  it('closes when clicking elsewhere without changing the choice',()=>{
    render(<RefreshProvider><RefreshControl onRefresh={vi.fn(async()=>{})}/><p>outside</p></RefreshProvider>);
    fireEvent.click(trigger());
    fireEvent.pointerDown(screen.getByText('outside'));
    expect(screen.queryByRole('listbox')).toBeNull();
    expect(trigger()).toHaveAccessibleName('Auto refresh: Off, page default');
  });
  it('explains itself when used outside its provider instead of failing silently',()=>{
    const spy=vi.spyOn(console,'error').mockImplementation(()=>{});
    expect(()=>render(<RefreshControl onRefresh={vi.fn(async()=>{})}/>)).toThrow(/RefreshProvider/);
    spy.mockRestore();
  });
});
