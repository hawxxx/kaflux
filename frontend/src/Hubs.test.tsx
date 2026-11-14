import {fireEvent,render,screen} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,beforeEach,describe,expect,it,vi} from 'vitest';
import {TopicsHub} from './Hubs';
import {allPermissions} from './test-permissions';
const session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:true};
const wrap=(ui:React.ReactNode)=>render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}>{ui}</QueryClientProvider>);
beforeEach(()=>{vi.stubGlobal('fetch',vi.fn(async(p:string)=>({ok:true,json:async()=>({data:p.endsWith('/brokers')?{brokers:[{id:1}]}:p.includes('/unused-topics')?{topics:[],evaluatedAt:'2026-11-13T00:00:00Z',partial:false}:[]})} as Response)))});
afterEach(()=>vi.restoreAllMocks());
describe('topics tabs',()=>{
  it('shows the topic list first and switches to cleanup',async()=>{
    wrap(<TopicsHub clusterId="demo" session={session} topics={<p>topic list</p>}/>);
    expect(screen.getAllByRole('tab').map(t=>t.textContent)).toEqual(['All topics','Cleanup']);
    expect(screen.getByText('topic list')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('tab',{name:'Cleanup'}));
    expect(await screen.findByRole('heading',{name:'Topic cleanup'})).toBeInTheDocument();
    expect(screen.getByText('Topics with no consumers and no recent writes. Review the reasons, then delete in bulk.')).toBeInTheDocument();
    expect(screen.queryByText('topic list')).toBeNull();
    fireEvent.click(screen.getByRole('tab',{name:'All topics'}));
    expect(screen.getByText('topic list')).toBeInTheDocument();
  });
});
