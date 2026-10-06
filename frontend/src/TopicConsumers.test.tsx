import {render,screen,within} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {TopicConsumers} from './TopicConsumers';
function mount(){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><TopicConsumers clusterId="demo" topic="orders"/></QueryClientProvider>)}
afterEach(()=>vi.restoreAllMocks());
describe('topic consumers',()=>{
  it('lists per-partition lag for each group consuming the topic',async()=>{
    const fetch=vi.fn(async()=>({ok:true,json:async()=>({data:[{id:'billing',state:'Stable',members:2,lag:7,offsets:[{topic:'orders',partition:0,committedOffset:10,startOffset:0,endOffset:15,lag:5},{topic:'orders',partition:1,committedOffset:-1,startOffset:3,endOffset:5,lag:2}]}]})} as Response));
    vi.stubGlobal('fetch',fetch);mount();
    const rows=await screen.findAllByRole('row');
    expect(String((fetch.mock.calls[0] as unknown[])[0])).toContain('/clusters/demo/topics/orders/consumers');
    expect(within(rows[1]).getByText('5')).toBeVisible();expect(within(rows[2]).getByText('Not committed')).toBeVisible();
    expect(screen.getByText('billing · Stable')).toBeVisible();
  });
  it('explains when no group consumes the topic',async()=>{
    vi.stubGlobal('fetch',vi.fn(async()=>({ok:true,json:async()=>({data:[]})} as Response)));mount();
    expect(await screen.findByText(/No consumer group has committed offsets/)).toBeVisible();
  });
});
