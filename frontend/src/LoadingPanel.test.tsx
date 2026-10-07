import {act,render,screen} from '@testing-library/react';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {LoadingPanel,UpdatingBar} from './LoadingPanel';

beforeEach(()=>vi.useFakeTimers());
afterEach(()=>vi.useRealTimers());

describe('LoadingPanel',()=>{
  it('names what is loading and offers an indeterminate progress bar',()=>{
    render(<LoadingPanel label="Loading topics"/>);
    expect(screen.getByRole('status')).toHaveTextContent('Loading topics…');
    const bar=screen.getByRole('progressbar',{name:'Loading topics'});
    expect(bar).not.toHaveAttribute('aria-valuenow');
  });
  it('keeps the previous wording when no label is given',()=>{
    render(<LoadingPanel/>);
    expect(screen.getByText('Retrieving cluster state…')).toBeInTheDocument();
  });
  it('shows elapsed time after a second and a slow hint only after the threshold',()=>{
    const {container}=render(<LoadingPanel label="Loading consumer groups"/>);
    expect(container.querySelector('em')).toBeNull();
    act(()=>{vi.advanceTimersByTime(2100)});
    expect(container.querySelector('em')).toHaveTextContent('2s');
    expect(screen.queryByText(/Still working/)).toBeNull();
    act(()=>{vi.advanceTimersByTime(2000)});
    expect(screen.getByText(/Still working/)).toBeInTheDocument();
  });
  it('does not announce the ticking counter to assistive technology',()=>{
    const {container}=render(<LoadingPanel/>);
    act(()=>{vi.advanceTimersByTime(3000)});
    expect(container.querySelector('em')).toHaveAttribute('aria-hidden','true');
  });
  it('stops its timer when it unmounts',()=>{
    const {unmount}=render(<LoadingPanel/>);
    expect(vi.getTimerCount()).toBeGreaterThan(0);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('UpdatingBar',()=>{
  it('is silent while idle',()=>{
    const {container}=render(<UpdatingBar active={false}/>);
    expect(container.querySelector('.updating-bar')).toHaveAttribute('data-active','false');
    expect(screen.queryByRole('status')).toBeNull();
  });
  it('ignores refreshes that finish quickly and reports slower ones',()=>{
    const {container,rerender}=render(<UpdatingBar active={true} label="Updating topics"/>);
    act(()=>{vi.advanceTimersByTime(250)});
    expect(screen.queryByRole('status')).toBeNull();
    rerender(<UpdatingBar active={false} label="Updating topics"/>);
    act(()=>{vi.advanceTimersByTime(1000)});
    expect(screen.queryByRole('status')).toBeNull();
    rerender(<UpdatingBar active={true} label="Updating topics"/>);
    act(()=>{vi.advanceTimersByTime(350)});
    expect(screen.getByRole('status')).toHaveTextContent('Updating topics…');
    expect(container.querySelector('.updating-bar')).toHaveAttribute('data-active','true');
    rerender(<UpdatingBar active={false} label="Updating topics"/>);
    expect(screen.queryByRole('status')).toBeNull();
  });
});
