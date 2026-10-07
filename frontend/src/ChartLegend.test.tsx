import {fireEvent,render,screen} from '@testing-library/react';
import {describe,expect,it,vi} from 'vitest';
import {ChartLegend,nextVisibility} from './ChartLegend';

describe('nextVisibility',()=>{
  it('a plain click isolates the series, and a second click shows everything again',()=>{
    const isolated=nextVisibility([true,true,true],1,false);
    expect(isolated).toEqual([false,true,false]);
    expect(nextVisibility(isolated,1,false)).toEqual([true,true,true]);
  });
  it('a plain click on a hidden series isolates that one instead',()=>{
    expect(nextVisibility([false,true,false],2,false)).toEqual([false,false,true]);
  });
  it('a modifier click adds or removes one series',()=>{
    expect(nextVisibility([false,true,false],0,true)).toEqual([true,true,false]);
    expect(nextVisibility([true,true,false],0,true)).toEqual([false,true,false]);
  });
  it('a modifier click never leaves the chart empty',()=>{
    expect(nextVisibility([false,true,false],1,true)).toEqual([true,true,true]);
  });
  it('ignores an index that is out of range',()=>{
    expect(nextVisibility([true,false],5,false)).toEqual([true,false]);
  });
});

describe('ChartLegend',()=>{
  const items=[{name:'consumer-group-with-a-really-long-name-that-would-not-fit-on-one-line',color:'--series-1'},{name:'payments',color:'--series-2'},{name:'Other'}];
  it('renders nothing for a single series',()=>{
    const {container}=render(<ChartLegend items={[items[0]]} visible={[true]} onToggle={()=>{}} onShowAll={()=>{}} onHover={()=>{}}/>);
    expect(container.firstChild).toBeNull();
  });
  it('lists every series with its full name available and its visibility state',()=>{
    render(<ChartLegend items={items} visible={[true,false,true]} onToggle={()=>{}} onShowAll={()=>{}} onHover={()=>{}}/>);
    const chips=screen.getAllByRole('button',{pressed:undefined}).filter(b=>b.classList.contains('chart-series-chip'));
    expect(chips).toHaveLength(3);
    expect(chips[0]).toHaveAttribute('title',expect.stringContaining(items[0].name));
    expect(chips[0]).toHaveTextContent(items[0].name);
    expect(chips[1]).toHaveAttribute('aria-pressed','false');
    expect(chips[2].querySelector('i')).toHaveClass('none');
  });
  it('reports clicks with the modifier state, hover for highlighting, and offers show all',()=>{
    const onToggle=vi.fn(),onHover=vi.fn(),onShowAll=vi.fn();
    render(<ChartLegend items={items} visible={[true,false,true]} onToggle={onToggle} onShowAll={onShowAll} onHover={onHover}/>);
    const chip=screen.getByRole('button',{name:/payments/});
    fireEvent.click(chip);fireEvent.click(chip,{ctrlKey:true});fireEvent.click(chip,{metaKey:true});
    expect(onToggle.mock.calls).toEqual([[1,false],[1,true],[1,true]]);
    fireEvent.mouseEnter(chip);fireEvent.mouseLeave(chip);
    expect(onHover.mock.calls).toEqual([[1],[null]]);
    fireEvent.click(screen.getByRole('button',{name:'Show all 3 series'}));
    expect(onShowAll).toHaveBeenCalledOnce();
  });
  it('hides the show all button when nothing is hidden',()=>{
    render(<ChartLegend items={items} visible={[true,true,true]} onToggle={()=>{}} onShowAll={()=>{}} onHover={()=>{}}/>);
    expect(screen.queryByRole('button',{name:/Show all/})).toBeNull();
  });
});
