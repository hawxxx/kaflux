import type {CSSProperties} from 'react';

export type LegendItem={name:string;color?:string};

/**
 * Visibility after a legend click, following Grafana's rules.
 * A plain click shows only the clicked series, or every series again when it was already the only one shown.
 * A modifier click (Ctrl, Cmd or Shift) adds or removes just that series, and never hides the last one.
 */
export function nextVisibility(visible:readonly boolean[],index:number,additive:boolean):boolean[]{
  if(index<0||index>=visible.length)return [...visible];
  if(additive){
    const next=visible.map((v,i)=>i===index?!v:v);
    return next.some(Boolean)?next:visible.map(()=>true);
  }
  const onlyThis=visible[index]&&visible.every((v,i)=>i===index||!v);
  return onlyThis?visible.map(()=>true):visible.map((_,i)=>i===index);
}

const safeColor=(color?:string)=>color&&/^--[a-z0-9-]+$/i.test(color)?`var(${color})`:undefined;

/**
 * Series legend below a chart. Names wrap onto as many lines as the panel allows and long names are
 * shortened with an ellipsis, so nothing runs off the side of the screen; the full name is in the
 * tooltip of each entry. Hovering an entry highlights its line, clicking isolates it.
 */
export function ChartLegend({items,visible,onToggle,onShowAll,onHover}:{items:LegendItem[];visible:readonly boolean[];onToggle:(index:number,additive:boolean)=>void;onShowAll:()=>void;onHover:(index:number|null)=>void}){
  if(items.length<2)return null;
  const hidden=visible.filter(v=>!v).length;
  return <div className="chart-series-legend">
    <div className="chart-series-legend-list" role="group" aria-label="Series">
      {items.map((item,i)=><button key={`${i}:${item.name}`} type="button" className="chart-series-chip" aria-pressed={visible[i]!==false}
        title={`${item.name}\nClick to show only this series. Ctrl, Cmd or Shift click to add or remove it.`}
        onClick={e=>onToggle(i,e.ctrlKey||e.metaKey||e.shiftKey)}
        onMouseEnter={()=>onHover(i)} onMouseLeave={()=>onHover(null)} onFocus={()=>onHover(i)} onBlur={()=>onHover(null)}>
        <i style={{background:safeColor(item.color)} as CSSProperties} className={safeColor(item.color)?undefined:'none'}/>
        <span>{item.name}</span>
      </button>)}
    </div>
    {hidden>0&&<button type="button" className="chart-series-reset" onClick={onShowAll}>Show all {items.length} series</button>}
  </div>;
}
