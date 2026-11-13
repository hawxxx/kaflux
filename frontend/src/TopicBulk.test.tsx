import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {TopicBulk,TopicSummary} from './TopicBulk';
const all=Array.from({length:230},(_,i)=>`t${i}`);
let requests:string[]=[];
function stub(names=all){requests=[];vi.stubGlobal('fetch',vi.fn(async(path:string)=>{requests.push(path);const u=new URL(path,'http://x');const q=u.searchParams.get('q')??'';const page=Number(u.searchParams.get('page'));const size=Number(u.searchParams.get('pageSize'));
  const list=names.filter(n=>n.includes(q));
  return {ok:true,json:async()=>({data:list.slice((page-1)*size,page*size).map(name=>({name})),meta:{total:list.length,page,pageSize:size}})} as Response}))}
afterEach(()=>vi.unstubAllGlobals());
describe('topic bulk actions',()=>{
  it('asks before adding more than 100 topics, then adds across pages without duplicates',async()=>{
    stub();const onChange=vi.fn();render(<TopicBulk clusterId="demo" selected={['t5']} onChange={onChange}/>);
    fireEvent.click(screen.getByRole('button',{name:'Add all topics'}));
    expect(await screen.findByRole('alertdialog')).toHaveTextContent('Add 229 topics?');expect(onChange).not.toHaveBeenCalled();
    expect(requests.every(p=>p.includes('internal=false'))).toBe(true);
    fireEvent.click(screen.getByRole('button',{name:'Add 229'}));
    const next=onChange.mock.calls[0][0] as string[];expect(next).toHaveLength(230);expect(next[0]).toBe('t5');expect(new Set(next).size).toBe(230);
  });
  it('declining the confirmation changes nothing',async()=>{
    stub();const onChange=vi.fn();render(<TopicBulk clusterId="demo" selected={[]} onChange={onChange}/>);
    fireEvent.click(screen.getByRole('button',{name:'Add all topics'}));await screen.findByRole('alertdialog');
    fireEvent.click(screen.getByRole('button',{name:'Cancel'}));
    expect(onChange).not.toHaveBeenCalled();expect(screen.getByRole('button',{name:'Add all topics'})).toBeVisible();
  });
  it('adds small sets immediately and only the ones matching the search',async()=>{
    stub(['orders','orders-dlq','payments']);const onChange=vi.fn();
    render(<TopicBulk clusterId="demo" selected={[]} onChange={onChange} search="order"/>);
    fireEvent.click(screen.getByRole('button',{name:'Add all matching “order”'}));
    await waitFor(()=>expect(onChange).toHaveBeenCalledWith(['orders','orders-dlq']));
    expect(requests[0]).toContain('q=order');
  });
  it('reports when nothing is left to add',async()=>{
    stub(['a']);render(<TopicBulk clusterId="demo" selected={['a']} onChange={vi.fn()}/>);
    fireEvent.click(screen.getByRole('button',{name:'Add all topics'}));expect(await screen.findByRole('alert')).toHaveTextContent('No more topics to add.');
  });
  it('can cancel a load in progress',async()=>{
    vi.stubGlobal('fetch',vi.fn((_p:string,init:RequestInit)=>new Promise((_,reject)=>init.signal?.addEventListener('abort',()=>reject(Object.assign(new Error('aborted'),{name:'AbortError'}))))));
    const onChange=vi.fn();render(<TopicBulk clusterId="demo" selected={[]} onChange={onChange}/>);
    fireEvent.click(screen.getByRole('button',{name:'Add all topics'}));expect(await screen.findByRole('status')).toHaveTextContent('Loading');
    fireEvent.click(screen.getByRole('button',{name:'Cancel'}));
    expect(await screen.findByRole('button',{name:'Add all topics'})).toBeVisible();expect(onChange).not.toHaveBeenCalled();expect(screen.queryByRole('alert')).toBeNull();
  });
  it('clears the selection and disables clear when empty',()=>{
    const onChange=vi.fn();const {rerender}=render(<TopicBulk clusterId="demo" selected={['a','b']} onChange={onChange}/>);
    fireEvent.click(screen.getByRole('button',{name:'Clear (2)'}));expect(onChange).toHaveBeenCalledWith([]);
    rerender(<TopicBulk clusterId="demo" selected={[]} onChange={onChange}/>);expect(screen.getByRole('button',{name:'Clear'})).toBeDisabled();
  });
  it('shows the error when loading topics fails',async()=>{
    vi.stubGlobal('fetch',vi.fn(async()=>({ok:false,status:500,statusText:'boom',json:async()=>({error:{code:'x',message:'boom'}})}) as Response));
    render(<TopicBulk clusterId="demo" selected={[]} onChange={vi.fn()}/>);fireEvent.click(screen.getByRole('button',{name:'Add all topics'}));
    expect(await screen.findByRole('alert')).toBeVisible();
  });
  it('summarises a large selection in run order',()=>{
    render(<TopicSummary topics={all}/>);expect(screen.getByText('230 topics selected · run in listed order')).toBeVisible();
    expect(screen.getAllByRole('listitem')).toHaveLength(230);
  });
});
