import {afterEach,describe,expect,it,vi} from 'vitest';
import {ApiError,api,apiRoot,bytes,latestSample,metricValue,prepareMetric,seriesNames,sparkTrend} from './api';

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
  it('reads the latest sample of an available series',()=>{
    expect(latestSample({status:'available',observedAt:'',series:[{points:[{time:1,value:4},{time:2,value:7}]}]})).toBe(7);
    expect(latestSample({status:'available',observedAt:'',series:[{values:[[1,2],[2,3]]}]})).toBe(3);
    expect(latestSample({status:'pending',observedAt:'',series:[]})).toBeUndefined();
    expect(latestSample({status:'available',observedAt:'',series:[]})).toBeUndefined();
  });
  it('sums every series per timestamp for sparkline trends',()=>{
    expect(sparkTrend({status:'available',observedAt:'',series:[{values:[[2,3],[1,1]]},{points:[{time:1,value:4},{time:2,value:5}]}]})).toEqual([5,8]);
    expect(sparkTrend({status:'pending',observedAt:'',series:[]})).toEqual([]);
  });
});

describe('seriesNames',()=>{
  const broker=(id:string,topic='orders')=>({labels:{__name__:'bytes_in',instance:`broker-${id}:9404`,topic},points:[]});
  it('fills the catalog legend template from series labels',()=>{
    expect(seriesNames([broker('1'),broker('2')],'{{instance}}')).toEqual(['broker-1:9404','broker-2:9404']);
  });
  it('falls back to the labels that differ when the legend is missing or ambiguous',()=>{
    expect(seriesNames([broker('1'),broker('2')])).toEqual(['broker-1:9404','broker-2:9404']);
    expect(seriesNames([broker('1','a'),broker('1','b')],'{{instance}}')).toEqual(['a','b']);
    expect(seriesNames([broker('1','a'),broker('2','b')],'Bytes in')).toEqual(['broker-1:9404 · a','broker-2:9404 · b']);
  });
  it('uses the metric title for a lone unlabeled series and numbers indistinguishable ones',()=>{
    expect(seriesNames([{points:[]}],'','Bytes in')).toEqual(['Bytes in']);
    expect(seriesNames([broker('1'),broker('1')])).toEqual(['Series 1','Series 2']);
  });
});

describe('prepareMetric',()=>{
  const line=(name:string,values:(number|null)[])=>({name,points:values.flatMap((value,i)=>value==null?[]:[{time:i*15,value}])});
  it('aligns series with gaps on shared timestamps and summarizes each one',()=>{
    const m=prepareMetric([line('a',[1,2,3]),line('b',[null,10,null])]);
    expect(m.timestamps).toEqual([0,15,30]);
    expect(m.all.map(s=>s.name)).toEqual(['b','a']);
    expect(m.all[1]).toMatchObject({values:[1,2,3],latest:3,average:2,peak:3});
    expect(m.all[0].values).toEqual([null,10,null]);
  });
  it('folds the long tail of additive units into Other',()=>{
    const m=prepareMetric(Array.from({length:10},(_,i)=>line(`t${i}`,[i,i])),{unit:'Bps'});
    expect(m.shown.map(s=>s.name)).toEqual(['t9','t8','t7','t6','t5','t4','t3']);
    expect(m.other).toMatchObject({name:'Other (3)',values:[3,3]});
    expect(m.all).toHaveLength(10);
  });
  it('truncates non-additive units without summing them',()=>{
    const m=prepareMetric(Array.from({length:10},(_,i)=>line(`t${i}`,[i])),{unit:'percent'});
    expect(m.shown).toHaveLength(8);
    expect(m.other).toBeUndefined();
  });
  it('assigns color slots by name so they do not follow rank',()=>{
    const m=prepareMetric([line('broker-10',[9]),line('broker-2',[1])]);
    expect([...m.slots]).toEqual([['broker-2',1],['broker-10',2]]);
  });
});

describe('apiRoot',()=>{
  it('returns the top-level API area of a path',()=>{
    expect(apiRoot('/clusters/c1/topics?q=a%20b')).toBe('clusters');
    expect(apiRoot('/auth/session')).toBe('auth');
  });
  it.each(['https://evil.example/x','//evil.example/x','clusters','/%2e%2e/admin','/../../admin','/a/../../b','/a\\b',''])('rejects %s',path=>{
    expect(()=>apiRoot(path)).toThrow(ApiError);
  });
});
describe('api',()=>{
  afterEach(()=>{vi.unstubAllGlobals()});
  it('requests allow-listed areas under the fixed base',async()=>{
    const fetchMock=vi.fn(async()=>new Response(JSON.stringify({data:1}),{status:200}));
    vi.stubGlobal('fetch',fetchMock);
    await expect(api('/clusters/c1/topics?q=a')).resolves.toEqual({data:1});
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/clusters/c1/topics?q=a',expect.objectContaining({credentials:'include'}));
  });
  it('never sends a request for an unknown area or an escaping path',async()=>{
    const fetchMock=vi.fn();
    vi.stubGlobal('fetch',fetchMock);
    await expect(api('/internal/secrets')).rejects.toMatchObject({code:'INVALID_PATH'});
    await expect(api('//evil.example/x')).rejects.toMatchObject({code:'INVALID_PATH'});
    expect(fetchMock).not.toHaveBeenCalled();
  });
});
