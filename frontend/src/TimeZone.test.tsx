import {act,fireEvent,render,screen} from '@testing-library/react';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {TimeZonePicker,formatInZone,loadZone,offsetLabel,parseInZone,setZone,useTimeZone} from './TimeZone';

function memoryStorage():Storage{const m=new Map<string,string>();return {get length(){return m.size},clear:()=>m.clear(),getItem:k=>m.get(k)??null,key:i=>[...m.keys()][i]??null,removeItem:k=>{m.delete(k)},setItem:(k,v)=>{m.set(k,String(v))}}}
beforeEach(()=>{vi.stubGlobal('localStorage',memoryStorage())});
afterEach(()=>{act(()=>setZone(null));vi.unstubAllGlobals()});

describe('offsets',()=>{
  it('labels offsets with sign and minutes',()=>{
    expect(offsetLabel(0)).toBe('UTC');
    expect(offsetLabel(330)).toBe('UTC+05:30');
    expect(offsetLabel(-210)).toBe('UTC-03:30');
    expect(offsetLabel(840)).toBe('UTC+14:00');
  });
  it('loads only offered offsets',()=>{
    expect(loadZone({getItem:()=>'345'})).toBe(345);
    expect(loadZone({getItem:()=>'17'})).toBeNull();
    expect(loadZone({getItem:()=>null})).toBeNull();
    expect(loadZone({getItem:()=>{throw new Error('blocked')}})).toBeNull();
  });
});

describe('formatting and parsing',()=>{
  const instant='2026-10-08T12:00:00.000Z';
  it('shows wall clock time at the offset and names it',()=>{
    expect(formatInZone(instant,330,'time')).toBe(new Date('2026-10-08T17:30:00Z').toLocaleTimeString(undefined,{timeZone:'UTC'}));
    expect(formatInZone(instant,0)).toMatch(/ UTC$/);
    expect(formatInZone(instant,-300)).toMatch(/ UTC-05:00$/);
  });
  it('keeps the browser format without a chosen zone',()=>{
    expect(formatInZone(instant,null)).toBe(new Date(instant).toLocaleString());
  });
  it('reads datetime-local input as time at the offset',()=>{
    expect(parseInZone('2026-10-08T17:30',330).toISOString()).toBe(instant);
    expect(parseInZone('2026-10-08T07:00:00',-300).toISOString()).toBe(instant);
    expect(parseInZone('2026-10-08T12:00',0).toISOString()).toBe(instant);
    expect(parseInZone('not a date',0).getTime()).toBeNaN();
  });
  it('round trips through format and parse',()=>{
    for(const zone of [-570,0,345,765]){
      const shifted=new Date(Date.parse(instant)+zone*60_000).toISOString().slice(0,16);
      expect(parseInZone(shifted,zone).toISOString()).toBe(instant);
    }
  });
});

function Shown(){const tz=useTimeZone();return <output>{tz.dateTime('2026-10-08T12:00:00Z')}</output>}

describe('picker',()=>{
  it('chooses a zone, updates every time on screen and remembers it',()=>{
    render(<><TimeZonePicker/><Shown/></>);
    expect(screen.getByRole('button',{name:/^Time zone: browser/})).toBeVisible();
    fireEvent.pointerDown(screen.getByRole('button',{name:/^Time zone/}),{button:0,ctrlKey:false});
    fireEvent.click(screen.getByRole('menuitemradio',{name:/UTC\+05:30/}));
    expect(screen.getByRole('button',{name:'Time zone: UTC+05:30'})).toBeVisible();
    expect(screen.getByRole('status')).toHaveTextContent(/ UTC\+05:30$/);
    expect(localStorage.getItem('kaflux-time-zone')).toBe('330');
  });
  it('returns to the browser zone',()=>{
    act(()=>setZone(0));
    render(<TimeZonePicker/>);
    fireEvent.pointerDown(screen.getByRole('button',{name:'Time zone: UTC'}),{button:0,ctrlKey:false});
    expect(screen.getByRole('menuitemradio',{name:/Coordinated Universal Time/})).toHaveAttribute('aria-checked','true');
    fireEvent.click(screen.getByRole('menuitemradio',{name:/Browser time/}));
    expect(screen.getByRole('button',{name:/^Time zone: browser/})).toBeVisible();
    expect(localStorage.getItem('kaflux-time-zone')).toBeNull();
  });
});
