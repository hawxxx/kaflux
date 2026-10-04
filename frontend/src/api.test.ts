import {describe,expect,it} from 'vitest';
import {bytes,metricValue} from './api';

describe('metric formatting',()=>{
  it('formats byte rates for chart axes',()=>{
    expect(metricValue(25_160_704,'Bps')).toBe('24.0 MiB/s');
    expect(metricValue(2048,'decbytes')).toBe('2.0 KiB');
  });
  it('rounds fractional small byte values',()=>{
    expect(bytes(23.456)).toBe('23.5 B');
    expect(bytes(12)).toBe('12 B');
  });
  it('scales percent units',()=>{
    expect(metricValue(0.25,'percentunit')).toBe('25%');
  });
});
