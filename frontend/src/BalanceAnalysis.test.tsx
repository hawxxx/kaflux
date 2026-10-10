import {render,screen} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {BalanceAnalysis} from './BalanceAnalysis';
import type {Dimension} from './TopicDistribution';

const dimension=(id:string,values:number[],status:Dimension['status']='HIGH_SKEW'):Dimension=>{const mean=values.reduce((a,b)=>a+b,0)/values.length;return {id,status,mean,maxMinusMin:Math.max(...values)-Math.min(...values),coefficientOfVariation:.3,maxToMeanRatio:Math.max(...values)/mean,brokers:values.map((value,i)=>({broker:i+1,value,differenceFromMean:value-mean,percentageDifference:(value-mean)/mean*100}))}};
const row=(id:number)=>screen.getByText(`broker-${id}`).closest('.broker-bar-row') as HTMLElement;

describe('balance analysis',()=>{
  it('shows the backend skew status and each broker deviation from the mean',()=>{
    render(<BalanceAnalysis dimension={dimension('replicas',[150,100,50])}/>);
    expect(screen.getByText('High skew')).toBeTruthy();
    expect(row(1).textContent).toContain('+50%');
    expect(row(2).textContent).not.toContain('%');
    expect(row(3).textContent).toContain('-50%');
  });

  it('colors brokers by position like the topic distribution view',()=>{
    render(<BalanceAnalysis dimension={dimension('leaders',[4,4,4,4],'GOOD')}/>);
    expect((row(1).querySelector('.bar-track>div') as HTMLElement).style.background).toBe('var(--series-1)');
    expect((row(4).querySelector('.bar-track>div') as HTMLElement).style.background).toBe('var(--series-4)');
  });

  it('marks the cluster mean on every bar',()=>{
    render(<BalanceAnalysis dimension={dimension('replicas',[150,100,50])}/>);
    expect((row(2).querySelector('.bar-track') as HTMLElement).style.getPropertyValue('--mean')).toBe(`${100/150*100}%`);
  });

  it('formats byte dimensions as sizes',()=>{
    render(<BalanceAnalysis dimension={dimension('bytes',[2*1024**3,1024**3])}/>);
    expect(row(1).textContent).toContain('2.0 GiB');
    expect(screen.getByText('1.5 GiB')).toBeTruthy();
  });
});
