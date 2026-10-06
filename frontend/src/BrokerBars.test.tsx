import {fireEvent,render,screen,within} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {BrokerBars,BROKERS_PER_PAGE} from './BrokerBars';
import type {Broker} from './api';

const brokers=(n:number,partitions=(i:number)=>100+i):Broker[]=>Array.from({length:n},(_,i)=>({id:i+1,host:`h${i+1}`,port:9092,rack:i%2?'az-b':'az-a',partitions:partitions(i),leaders:50,sizeBytes:null}) as Broker);
// The label span also holds the rack, so read only its own text (the broker name).
const names=()=>Array.from(document.querySelectorAll('.broker-bar-row>span')).map(e=>Array.from(e.childNodes).filter(n=>n.nodeType===Node.TEXT_NODE).map(n=>n.textContent).join(''));
const barWidth=(id:number)=>{const row=screen.getByText(`broker-${id}`).closest('.broker-bar-row') as HTMLElement;return (row.querySelector('.bar-track>div') as HTMLElement).style.width};
const barColor=(id:number)=>{const row=screen.getByText(`broker-${id}`).closest('.broker-bar-row') as HTMLElement;return (row.querySelector('.bar-track>div') as HTMLElement).style.background};

describe('broker distribution paging',()=>{
  it('shows every broker and no pager for a cluster that fits on one page',()=>{
    render(<BrokerBars brokers={brokers(BROKERS_PER_PAGE)} mode="KRaft"/>);
    expect(names()).toHaveLength(BROKERS_PER_PAGE);
    expect(screen.queryByRole('status')).toBeNull();
    expect(screen.queryByRole('button')).toBeNull();
    expect(screen.getByText(`${BROKERS_PER_PAGE} brokers · KRaft`)).toBeInTheDocument();
  });

  it('shows one page at a time and moves through the pages',()=>{
    render(<BrokerBars brokers={brokers(25)} mode="KRaft"/>);
    expect(names()).toEqual(Array.from({length:10},(_,i)=>`broker-${i+1}`));
    expect(screen.getByRole('status')).toHaveTextContent('Showing 1–10 of 25 brokers');
    expect(screen.getByText('Page 1 of 3')).toBeInTheDocument();
    expect(screen.getByRole('button',{name:'Previous brokers'})).toBeDisabled();

    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    expect(names()).toEqual(Array.from({length:10},(_,i)=>`broker-${i+11}`));
    expect(screen.getByRole('status')).toHaveTextContent('Showing 11–20 of 25 brokers');

    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    expect(names()).toEqual(Array.from({length:5},(_,i)=>`broker-${i+21}`));
    expect(screen.getByRole('status')).toHaveTextContent('Showing 21–25 of 25 brokers');
    expect(screen.getByRole('button',{name:'Next brokers'})).toBeDisabled();

    fireEvent.click(screen.getByRole('button',{name:'Previous brokers'}));
    expect(screen.getByText('Page 2 of 3')).toBeInTheDocument();
  });

  it('expands to every broker and collapses back to the first page',()=>{
    render(<BrokerBars brokers={brokers(25)} mode="KRaft"/>);
    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    const toggle=screen.getByRole('button',{name:'Show all 25'});
    expect(toggle).toHaveAttribute('aria-expanded','false');
    fireEvent.click(toggle);

    expect(names()).toHaveLength(25);
    expect(screen.getByRole('status')).toHaveTextContent('Showing all 25 brokers');
    expect(screen.queryByRole('button',{name:'Next brokers'})).toBeNull();
    expect(screen.getByRole('button',{name:'Show fewer'})).toHaveAttribute('aria-expanded','true');

    fireEvent.click(screen.getByRole('button',{name:'Show fewer'}));
    expect(names()).toHaveLength(10);
    expect(names()[0]).toBe('broker-1');
    expect(screen.getByText('Page 1 of 3')).toBeInTheDocument();
  });

  it('keeps each broker bar length and colour the same on every page',()=>{
    // Broker 1 is the busiest, so it sets 100% for the whole cluster. Broker 12 is on page 2.
    const list=brokers(25,i=>i===0?1000:(i===11?500:100));
    const {unmount}=render(<BrokerBars brokers={list} mode="KRaft"/>);
    expect(barWidth(1)).toBe('100%');
    expect(barColor(1)).toBe('var(--broker-0)');
    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    expect(barWidth(12)).toBe('50%');
    expect(barColor(12)).toBe('var(--broker-2)');
    expect(barColor(11)).toBe('var(--broker-1)');
    unmount();
    render(<BrokerBars brokers={list} mode="KRaft"/>);
    fireEvent.click(screen.getByRole('button',{name:'Show all 25'}));
    expect(barWidth(12)).toBe('50%');
    expect(barColor(12)).toBe('var(--broker-2)');
  });

  it('keeps a minimum visible bar for an idle broker',()=>{
    render(<BrokerBars brokers={[...brokers(2,i=>i===0?1000:0)]} mode="KRaft"/>);
    expect(barWidth(2)).toBe('2%');
  });

  it('moves back onto the last page when a refresh removes brokers',()=>{
    const {rerender}=render(<BrokerBars brokers={brokers(25)} mode="KRaft"/>);
    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    fireEvent.click(screen.getByRole('button',{name:'Next brokers'}));
    expect(screen.getByRole('status')).toHaveTextContent('Showing 21–25 of 25 brokers');
    rerender(<BrokerBars brokers={brokers(12)} mode="KRaft"/>);
    expect(screen.getByRole('status')).toHaveTextContent('Showing 11–12 of 12 brokers');
    expect(names()).toEqual(['broker-11','broker-12']);
    rerender(<BrokerBars brokers={brokers(8)} mode="KRaft"/>);
    expect(names()).toHaveLength(8);
    expect(screen.queryByRole('status')).toBeNull();
  });

  it('labels the rack of each broker and handles a missing rack',()=>{
    const list=brokers(2);
    (list[1] as {rack:string|null}).rack=null;
    render(<BrokerBars brokers={list} mode="KRaft"/>);
    expect(within(screen.getByText('broker-1').closest('.broker-bar-row') as HTMLElement).getByText('az-a')).toBeInTheDocument();
    expect(screen.getByText('Rack unavailable')).toBeInTheDocument();
  });

  it('honours a custom page size',()=>{
    render(<BrokerBars brokers={brokers(7)} mode="KRaft" pageSize={3}/>);
    expect(names()).toHaveLength(3);
    expect(screen.getByText('Page 1 of 3')).toBeInTheDocument();
  });
});
