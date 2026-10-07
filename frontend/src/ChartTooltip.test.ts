import {describe,it,expect,vi} from 'vitest';
import {buildTooltipRows,truncateName,placeTooltip,hoverTooltipPlugin} from './ChartTooltip';

describe('buildTooltipRows',()=>{
  it('sorts visible series by value descending',()=>{
    const series=[{name:'broker-1'},{name:'broker-2'},{name:'broker-3'}];
    const {rows}=buildTooltipRows(series,[10,30,20]);
    expect(rows.map(r=>r.name)).toEqual(['broker-2','broker-3','broker-1']);
  });

  it('sorts null values to the end regardless of position',()=>{
    const series=[{name:'a'},{name:'b'},{name:'c'}];
    const {rows}=buildTooltipRows(series,[null,5,null]);
    expect(rows.map(r=>r.name)).toEqual(['b','a','c']);
    expect(rows[1].value).toBeNull();
    expect(rows[2].value).toBeNull();
  });

  it('excludes series that are hidden (show:false)',()=>{
    const series=[{name:'a',show:true},{name:'b',show:false},{name:'c',show:true}];
    const {rows}=buildTooltipRows(series,[1,2,3]);
    expect(rows.map(r=>r.name)).toEqual(['c','a']);
  });

  it('treats series.show undefined as visible',()=>{
    const series=[{name:'a'},{name:'b',show:true}];
    const {rows}=buildTooltipRows(series,[1,2]);
    expect(rows).toHaveLength(2);
  });

  it('caps rows at 12 and reports the remainder as moreCount',()=>{
    const series=Array.from({length:15},(_,i)=>({name:`s${i}`}));
    const values=series.map((_,i)=>i);
    const {rows,moreCount}=buildTooltipRows(series,values);
    expect(rows).toHaveLength(12);
    expect(moreCount).toBe(3);
    // highest values retained first
    expect(rows[0].name).toBe('s14');
  });

  it('reports zero moreCount when at or under the cap',()=>{
    const series=Array.from({length:12},(_,i)=>({name:`s${i}`}));
    const {rows,moreCount}=buildTooltipRows(series,series.map((_,i)=>i));
    expect(rows).toHaveLength(12);
    expect(moreCount).toBe(0);
  });

  it('carries color through to each row',()=>{
    const series=[{name:'a',color:'--series-1'}];
    const {rows}=buildTooltipRows(series,[5]);
    expect(rows[0].color).toBe('--series-1');
  });

  it('returns no rows when all series are hidden',()=>{
    const series=[{name:'a',show:false},{name:'b',show:false}];
    const {rows,moreCount}=buildTooltipRows(series,[1,2]);
    expect(rows).toHaveLength(0);
    expect(moreCount).toBe(0);
  });
});

describe('truncateName',()=>{
  it('leaves short names unchanged',()=>{
    expect(truncateName('broker-1')).toBe('broker-1');
  });

  it('truncates long names with an ellipsis at the configured length',()=>{
    const name='a-very-long-broker-instance-name-that-overflows';
    const result=truncateName(name,20);
    expect(result).toHaveLength(20);
    expect(result.endsWith('…')).toBe(true);
    expect(name.startsWith(result.slice(0,-1))).toBe(true);
  });

  it('uses a default max length when none is given',()=>{
    const name='x'.repeat(50);
    const result=truncateName(name);
    expect(result.length).toBeLessThan(name.length);
    expect(result.endsWith('…')).toBe(true);
  });

  it('does not truncate a name exactly at the max length',()=>{
    const name='x'.repeat(28);
    expect(truncateName(name,28)).toBe(name);
  });
});

describe('placeTooltip',()=>{
  const base={chartWidth:600,chartHeight:300,tooltipWidth:150,tooltipHeight:100};

  it('places the tooltip to the right and below the cursor by default',()=>{
    const pos=placeTooltip({...base,cursorLeft:50,cursorTop:50});
    expect(pos.left).toBeGreaterThan(50);
    expect(pos.top).toBeGreaterThan(50);
  });

  it('flips to the left when the right placement would overflow the chart width',()=>{
    const pos=placeTooltip({...base,cursorLeft:560,cursorTop:50});
    expect(pos.left).toBeLessThan(560);
    expect(pos.left+base.tooltipWidth).toBeLessThanOrEqual(base.chartWidth+1);
  });

  it('flips upward when the bottom placement would overflow the chart height',()=>{
    const pos=placeTooltip({...base,cursorLeft:50,cursorTop:280});
    expect(pos.top).toBeLessThan(280);
    expect(pos.top+base.tooltipHeight).toBeLessThanOrEqual(base.chartHeight+1);
  });

  it('flips both axes near the bottom-right corner',()=>{
    const pos=placeTooltip({...base,cursorLeft:580,cursorTop:280});
    expect(pos.left).toBeLessThan(580);
    expect(pos.top).toBeLessThan(280);
  });

  it('clamps within bounds even when the tooltip is wider than the chart',()=>{
    const pos=placeTooltip({...base,tooltipWidth:900,cursorLeft:50,cursorTop:50});
    expect(pos.left).toBeGreaterThanOrEqual(0);
  });

  it('respects a custom offset from the cursor',()=>{
    const a=placeTooltip({...base,cursorLeft:50,cursorTop:50,offset:5});
    const b=placeTooltip({...base,cursorLeft:50,cursorTop:50,offset:30});
    expect(b.left).toBeGreaterThan(a.left);
  });
});

describe('hoverTooltipPlugin rendering',()=>{
  it('renders hostile series names as text and ignores unsafe colors',()=>{
    const root=document.createElement('div');const over=document.createElement('div');root.append(over);
    const name='<img src=x onerror="window.__xss=1">orders';
    const plugin=hoverTooltipPlugin({getSeries:()=>[{name,color:'--series-1',show:true},{name:'payments',color:'red;background:url(//evil)',show:true}],formatValue:(v:number)=>`${v} msg/s`,formatTime:()=>'12:00'});
    const u={root,over,cursor:{idx:0,left:10,top:10},data:[[1],[5],[3]],bbox:{width:400,height:200}};
    const run=(hooks:unknown)=>{for(const h of [hooks].flat())(h as (x:unknown)=>void)?.(u)};
    document.querySelectorAll('.chart-tooltip').forEach(e=>e.remove());
    run(plugin.hooks.ready);run(plugin.hooks.setCursor);
    const tip=document.body.querySelector('.chart-tooltip')!;
    expect(tip.querySelector('img')).toBeNull();
    expect((window as unknown as {__xss?:number}).__xss).toBeUndefined();
    expect(tip.textContent).toContain('<img src=x');
    expect(tip.textContent).toContain('5 msg/s');
    const swatches=[...tip.querySelectorAll('i')] as HTMLElement[];
    expect(swatches[0].style.background).toBe('var(--series-1)');
    expect(swatches[1].style.background).toBe('');
    run(plugin.hooks.destroy);
  });
});

describe('hoverTooltipPlugin placement',()=>{
  function mount(rect:{left:number;top:number},cursor:{left:number;top:number}){
    document.querySelectorAll('.chart-tooltip').forEach(e=>e.remove());
    const root=document.createElement('div');const over=document.createElement('div');root.append(over);document.body.append(root);
    over.getBoundingClientRect=()=>({left:rect.left,top:rect.top,right:rect.left+400,bottom:rect.top+200,width:400,height:200,x:rect.left,y:rect.top,toJSON:()=>({})});
    const plugin=hoverTooltipPlugin({getSeries:()=>[{name:'broker-1',color:'--series-1'},{name:'broker-2',color:'--series-2'}],formatValue:(v:number)=>String(v),formatTime:()=>'12:00'});
    const u={root,over,cursor:{idx:0,...cursor},data:[[1],[5],[3]],bbox:{width:800,height:400}};
    const run=(hooks:unknown,...args:unknown[])=>{for(const h of [hooks].flat())(h as (...x:unknown[])=>void)?.(u,...args)};
    return {plugin,u,run,over,root};
  }
  it('attaches to the document body with fixed positioning, independent of the chart ancestors',()=>{
    const {plugin,run}=mount({left:300,top:500},{left:20,top:30});
    run(plugin.hooks.ready);run(plugin.hooks.setCursor);
    const tip=document.body.querySelector('.chart-tooltip') as HTMLElement;
    expect(tip.parentElement).toBe(document.body);
    expect(tip.style.position).toBe('fixed');
    // plot origin (300,500) plus cursor (20,30) plus the 14 px offset; jsdom reports a zero size tooltip
    expect(tip.style.transform).toBe('translate(334px,544px)');
    run(plugin.hooks.destroy);
  });
  it('hides on scroll and removes itself and its listeners on destroy',()=>{
    const {plugin,run}=mount({left:10,top:10},{left:5,top:5});
    run(plugin.hooks.ready);run(plugin.hooks.setCursor);
    const tip=document.body.querySelector('.chart-tooltip') as HTMLElement;
    expect(tip.style.visibility).toBe('visible');
    window.dispatchEvent(new Event('scroll'));
    expect(tip.style.visibility).toBe('hidden');
    const removed=vi.spyOn(window,'removeEventListener');
    run(plugin.hooks.destroy);
    expect(document.body.querySelector('.chart-tooltip')).toBeNull();
    expect(removed.mock.calls.map(c=>c[0])).toEqual(expect.arrayContaining(['scroll','blur']));
    removed.mockRestore();
  });
  it('marks the row of the series the cursor focuses',()=>{
    const {plugin,run}=mount({left:10,top:10},{left:5,top:5});
    run(plugin.hooks.ready);run(plugin.hooks.setCursor);
    run(plugin.hooks.setSeries,2,{focus:true});
    const tip=document.body.querySelector('.chart-tooltip') as HTMLElement;
    expect(tip.querySelector('li.is-focus')?.textContent).toContain('broker-2');
    run(plugin.hooks.setSeries,null,{focus:true});
    expect(tip.querySelector('li.is-focus')).toBeNull();
    run(plugin.hooks.destroy);
  });
});

describe('placeTooltip viewport margin',()=>{
  it('keeps a margin from every edge and flips near the right and bottom',()=>{
    expect(placeTooltip({cursorLeft:990,cursorTop:790,chartWidth:1000,chartHeight:800,tooltipWidth:200,tooltipHeight:100,offset:14,margin:8})).toEqual({left:776,top:676});
    expect(placeTooltip({cursorLeft:2,cursorTop:2,chartWidth:1000,chartHeight:800,tooltipWidth:200,tooltipHeight:100,offset:14,margin:8})).toEqual({left:16,top:16});
    expect(placeTooltip({cursorLeft:100,cursorTop:100,chartWidth:150,chartHeight:120,tooltipWidth:200,tooltipHeight:200,offset:14,margin:8})).toEqual({left:8,top:8});
  });
});

describe('hoverTooltipPlugin lifecycle',()=>{
  it('creates nothing when the chart is destroyed before uPlot reports ready',()=>{
    document.querySelectorAll('.chart-tooltip').forEach(e=>e.remove());
    const root=document.createElement('div');const over=document.createElement('div');root.append(over);
    const plugin=hoverTooltipPlugin({getSeries:()=>[{name:'broker-1'}],formatValue:(v:number)=>String(v),formatTime:()=>''});
    const u={root,over,cursor:{idx:0,left:1,top:1},data:[[1],[1]],bbox:{width:10,height:10}};
    const added=vi.spyOn(window,'addEventListener');
    for(const h of [plugin.hooks.destroy].flat())(h as (x:unknown)=>void)?.(u);
    for(const h of [plugin.hooks.ready].flat())(h as (x:unknown)=>void)?.(u);
    expect(document.querySelectorAll('.chart-tooltip')).toHaveLength(0);
    expect(added.mock.calls.filter(c=>c[0]==='scroll')).toHaveLength(0);
    added.mockRestore();
  });
});
