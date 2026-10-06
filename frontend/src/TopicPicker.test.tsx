import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {TopicPicker} from './TopicPicker';
const all=['orders','orders-dlq','payments','users'];
function mount(value=''){const onChange=vi.fn();const requests:string[]=[];
  vi.stubGlobal('fetch',vi.fn(async(path:string)=>{requests.push(path);const q=new URL(path,'http://x').searchParams.get('q')??'';const data=all.filter(n=>n.includes(q)).map(name=>({name}));return {ok:true,json:async()=>({data,meta:{total:data.length,page:1,pageSize:50}})} as Response}));
  render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><label>Topic<TopicPicker clusterId="demo" value={value} onChange={onChange}/></label></QueryClientProvider>);
  return {onChange,requests,input:screen.getByRole('combobox',{name:'Topic'})}}
afterEach(()=>vi.unstubAllGlobals());
describe('topic picker',()=>{
  it('lists topics on focus and filters through the server search',async()=>{
    const {input,requests}=mount();fireEvent.focus(input);
    expect(await screen.findByRole('option',{name:'users'})).toBeVisible();
    fireEvent.change(input,{target:{value:'order'}});
    await waitFor(()=>expect(screen.queryByRole('option',{name:'users'})).toBeNull());
    expect(screen.getAllByRole('option').map(o=>o.textContent)).toEqual(['orders','orders-dlq']);
    expect(requests.some(p=>p.includes('q=order'))).toBe(true);
  });
  it('selects with the keyboard and closes',async()=>{
    const {input,onChange}=mount();fireEvent.focus(input);await screen.findByRole('option',{name:'orders'});
    fireEvent.keyDown(input,{key:'ArrowDown'});fireEvent.keyDown(input,{key:'Enter'});
    expect(onChange).toHaveBeenCalledWith('orders-dlq');expect(input).toHaveValue('orders-dlq');expect(screen.queryByRole('listbox')).toBeNull();
  });
  it('selects with a click inside a label without reopening',async()=>{
    const {input,onChange}=mount();fireEvent.focus(input);fireEvent.click(await screen.findByRole('option',{name:'payments'}));
    expect(onChange).toHaveBeenCalledWith('payments');expect(screen.queryByRole('listbox')).toBeNull();
  });
  it('restores the chosen topic when search is abandoned',async()=>{
    const {input,onChange}=mount('users');fireEvent.focus(input);fireEvent.change(input,{target:{value:'pay'}});fireEvent.keyDown(input,{key:'Escape'});
    expect(input).toHaveValue('users');expect(onChange).not.toHaveBeenCalled();
  });
});
