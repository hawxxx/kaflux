import type uPlot from 'uplot';

export type TooltipRow={name:string;color?:string;value:number|null;isOther?:boolean};
export type TooltipSeriesInput={name:string;color?:string;show?:boolean};

const MAX_ROWS=12;
// Long enough for real topic and consumer group names; anything longer still wraps in the tooltip.
const MAX_NAME=120;

/** Builds the rows shown for one cursor position: one row per visible series, sorted by value
 * descending (nulls sort last), truncated to MAX_ROWS with a "+N more" marker carried on the result. */
export function buildTooltipRows(series:TooltipSeriesInput[],values:(number|null)[]):{rows:TooltipRow[];moreCount:number}{
  const all=series.map((s,i):TooltipRow=>({name:s.name,color:s.color,value:values[i]??null})).filter((_,i)=>series[i].show!==false);
  all.sort((a,b)=>{
    if(a.value==null&&b.value==null)return 0;
    if(a.value==null)return 1;
    if(b.value==null)return -1;
    return b.value-a.value;
  });
  const rows=all.slice(0,MAX_ROWS);
  const moreCount=Math.max(0,all.length-MAX_ROWS);
  return {rows,moreCount};
}

/** Truncates a series name to a maximum character length, preserving the full value for a title attribute. */
export function truncateName(name:string,max=28):string{
  if(name.length<=max)return name;
  return `${name.slice(0,Math.max(0,max-1))}…`;
}

export type TooltipPosition={left:number;top:number};

/** Picks the tooltip's placement so it stays inside [0,width] x [0,height], flipping to the
 * opposite side of the cursor when the default placement (right and below) would overflow. */
export function placeTooltip(opts:{
  cursorLeft:number;cursorTop:number;chartWidth:number;chartHeight:number;
  tooltipWidth:number;tooltipHeight:number;offset?:number;margin?:number;
}):TooltipPosition{
  const {cursorLeft,cursorTop,chartWidth,chartHeight,tooltipWidth,tooltipHeight}=opts;
  const offset=opts.offset??12;
  const margin=opts.margin??0;
  const clamp=(v:number,size:number,limit:number)=>Math.max(margin,Math.min(v,limit-margin-size));
  let left=cursorLeft+offset;
  if(left+tooltipWidth>chartWidth-margin)left=cursorLeft-offset-tooltipWidth;
  left=clamp(left,tooltipWidth,chartWidth);
  let top=cursorTop+offset;
  if(top+tooltipHeight>chartHeight-margin)top=cursorTop-offset-tooltipHeight;
  top=clamp(top,tooltipHeight,chartHeight);
  return {left,top};
}

export type HoverTooltipOptions={
  getSeries:()=>TooltipSeriesInput[];
  formatValue:(value:number)=>string;
  formatTime:(timestamp:number)=>string;
  onFocus?:(seriesIdx:number|null)=>void;
};

/** Grafana-style hover plugin: vertical crosshair (native cursor.x) plus a floating tooltip
 * listing every visible series at the hovered timestamp, sorted by value descending.
 * The tooltip element lives outside uPlot's DOM updates and is positioned on every setCursor. */
export function hoverTooltipPlugin(options:HoverTooltipOptions):uPlot.Plugin{
  let tooltip:HTMLDivElement|null=null;
  let over:HTMLDivElement|null=null;

  // The tooltip is attached to the document body and positioned against the viewport. Placing it
  // inside the chart made its position depend on which ancestor happened to be positioned or
  // transformed, so the same chart rendered correctly on one page and in the corner of another.
  let focused:string|null=null;
  // uPlot fires ready after the current task, so a chart destroyed in its first frame (React
  // re-running an effect, a quick page change) would otherwise create a tooltip nobody removes.
  let destroyed=false;
  const hide=()=>{if(tooltip)tooltip.style.visibility='hidden'};
  function ensureTooltip():HTMLDivElement{
    if(tooltip)return tooltip;
    tooltip=document.createElement('div');
    tooltip.className='chart-tooltip';
    tooltip.setAttribute('role','tooltip');
    tooltip.setAttribute('aria-hidden','true');
    tooltip.style.position='fixed';
    tooltip.style.left='0';
    tooltip.style.top='0';
    tooltip.style.pointerEvents='none';
    tooltip.style.visibility='hidden';
    document.body.appendChild(tooltip);
    window.addEventListener('scroll',hide,true);
    window.addEventListener('blur',hide);
    return tooltip;
  }

  function render(u:uPlot){
    const tip=tooltip;
    if(!tip)return;
    const idx=u.cursor.idx;
    if(idx==null||u.cursor.left==null||u.cursor.left<0){
      tip.style.visibility='hidden';
      options.onFocus?.(null);
      return;
    }
    const timestamp=u.data[0][idx] as number;
    const series=options.getSeries();
    const values=series.map((_,i)=>u.data[i+1][idx] as number|null);
    const {rows,moreCount}=buildTooltipRows(series,values);
    if(!rows.length){
      tip.style.visibility='hidden';
      return;
    }
    // Series names are topic, group or host names chosen by users, so the tooltip is built from
    // nodes with textContent and never parsed as HTML.
    const el=(tag:string,className?:string,text?:string)=>{const node=document.createElement(tag);if(className)node.className=className;if(text!=null)node.textContent=text;return node};
    const list=el('ul');
    for(const r of rows){
      const item=el('li',r.name===focused?'is-focus':undefined);item.title=r.name;
      const swatch=el('i',r.color?undefined:'none');
      if(r.color&&/^--[a-z0-9-]+$/i.test(r.color))swatch.style.background=`var(${r.color})`;
      item.append(swatch,el('span','chart-tooltip-name',truncateName(r.name,MAX_NAME)),el('span','chart-tooltip-value',r.value==null?'No data':options.formatValue(r.value)));
      list.append(item);
    }
    if(moreCount>0)list.append(el('li','chart-tooltip-more',`+${moreCount} more`));
    tip.replaceChildren(el('div','chart-tooltip-time',options.formatTime(timestamp)),list);
    tip.style.visibility='visible';
    const width=tip.offsetWidth;
    const height=tip.offsetHeight;
    // Cursor coordinates are relative to the plotting area; convert them to the viewport and keep
    // the tooltip on screen, flipping to the other side of the cursor near an edge.
    const rect=u.over.getBoundingClientRect();
    const pos=placeTooltip({cursorLeft:rect.left+u.cursor.left,cursorTop:rect.top+(u.cursor.top??0),chartWidth:document.documentElement.clientWidth||window.innerWidth,chartHeight:window.innerHeight,tooltipWidth:width,tooltipHeight:height,offset:14,margin:8});
    tip.style.transform=`translate(${Math.round(pos.left)}px,${Math.round(pos.top)}px)`;
  }

  return {
    hooks:{
      ready:[(u:uPlot)=>{
        if(destroyed)return;
        over=u.over;
        ensureTooltip();
        over.addEventListener('mouseleave',()=>{hide();options.onFocus?.(null)});
      }],
      setCursor:[(u:uPlot)=>render(u)],
      setSeries:[(u:uPlot,seriesIdx:number|null,opts:uPlot.Series)=>{
        // uPlot reports focus changes here; remember which line is emphasized so its row stands out.
        if(opts&&'focus' in opts)focused=seriesIdx==null?null:options.getSeries()[seriesIdx-1]?.name??null;
        options.onFocus?.(seriesIdx);
        if(tooltip?.style.visibility==='visible')render(u);
      }],
      destroy:[()=>{destroyed=true;window.removeEventListener('scroll',hide,true);window.removeEventListener('blur',hide);tooltip?.remove();tooltip=null;over=null}],
    },
  };
}

