import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {ConsumerGroups} from './ConsumerGroups';
import {allPermissions} from './test-permissions';
const session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:true};
function mount(){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><ConsumerGroups clusterId="demo" session={session}/></QueryClientProvider>)}
afterEach(()=>vi.restoreAllMocks());
describe('consumer offset administration',()=>{
  it('previews offsets, requires exact group confirmation, and sends the reviewed hash',async()=>{
    const requests:{path:string;body:any}[]=[];
    vi.stubGlobal('fetch',vi.fn(async(path:string,options?:RequestInit)=>{const body=options?.body?JSON.parse(String(options.body)):null;requests.push({path,body});return {ok:true,json:async()=>({data:path.endsWith('/reset-offsets')?{groupId:'idle-workers',changes:[{topic:'events',partition:0,before:12,after:0}],previewHash:'reviewed-hash',applied:!body.preview}:path.endsWith('/idle-workers')?{id:'idle-workers',state:'Empty',members:0,offsets:[{topic:'events',partition:0,committedOffset:12,startOffset:0,endOffset:20,lag:8}]}:[{id:'idle-workers',state:'Empty',members:0,lag:8}]})} as Response}));
    mount();fireEvent.click(await screen.findByRole('button',{name:'idle-workers'}));
    await screen.findByText('Committed offset');fireEvent.click(screen.getByRole('button',{name:'Reset offsets'}));
    fireEvent.change(screen.getByLabelText('Reset strategy'),{target:{value:'earliest'}});
    fireEvent.click(screen.getByRole('button',{name:'Preview offset changes'}));
    await screen.findByText('12 → 0');expect(screen.getByRole('button',{name:'Confirm offset reset'})).toBeDisabled();
    fireEvent.change(screen.getByLabelText('Type the group ID to confirm'),{target:{value:'idle-workers'}});
    fireEvent.click(screen.getByRole('button',{name:'Confirm offset reset'}));
    await waitFor(()=>expect(requests.some(r=>r.body?.preview===false&&r.body.previewHash==='reviewed-hash'&&r.body.confirmation==='idle-workers')).toBe(true));
  });
  it('shows broker authorization errors rather than a successful reset',async()=>{
    vi.stubGlobal('fetch',vi.fn(async(path:string)=>({ok:!path.endsWith('/reset-offsets'),status:403,json:async()=>path.endsWith('/reset-offsets')?{error:{message:'Missing offsets:reset permission',code:'FORBIDDEN'}}:{data:path.endsWith('/idle-workers')?{id:'idle-workers',state:'Empty',members:0,offsets:[]}:[{id:'idle-workers',state:'Empty',members:0,lag:0}]}} as Response)));
    mount();fireEvent.click(await screen.findByRole('button',{name:'idle-workers'}));fireEvent.click(await screen.findByRole('button',{name:'Reset offsets'}));fireEvent.click(screen.getByRole('button',{name:'Preview offset changes'}));expect(await screen.findByRole('alert')).toHaveTextContent('Missing offsets:reset permission');
  });
  it('disables reset for an active group even for an administrator',async()=>{
    vi.stubGlobal('fetch',vi.fn(async(path:string)=>({ok:true,json:async()=>({data:path.endsWith('/workers')?{id:'workers',state:'Stable',members:2,offsets:[]}:[{id:'workers',state:'Stable',members:2,lag:0}]})} as Response)));
    mount();fireEvent.click(await screen.findByRole('button',{name:'workers'}));expect(await screen.findByRole('button',{name:'Reset offsets'})).toBeDisabled();expect(screen.getByText(/Stop all consumers/)).toBeVisible();
  });
  it('shows a named progress indicator while the group list loads, then the table',async()=>{
    let release:(v:unknown)=>void=()=>{};
    const pending=new Promise(r=>{release=r});
    vi.stubGlobal('fetch',vi.fn(async()=>{await pending;return {ok:true,json:async()=>({data:[{id:'billing',state:'Stable',members:3,lag:12,topics:[]}]})}}));
    mount();
    expect(screen.getByRole('status')).toHaveTextContent('Loading consumer groups…');
    expect(screen.getByRole('progressbar',{name:'Loading consumer groups'})).toBeInTheDocument();
    expect(screen.queryByRole('table')).toBeNull();
    expect(screen.getByLabelText('Filter consumer groups')).toBeInTheDocument();
    release(null);
    expect(await screen.findByRole('button',{name:'billing'})).toBeInTheDocument();
    await waitFor(()=>expect(screen.queryByRole('progressbar')).toBeNull());
  });
  it('shows a progress indicator while a selected group loads its offsets',async()=>{
    let release:(v:unknown)=>void=()=>{};
    const pending=new Promise(r=>{release=r});
    vi.stubGlobal('fetch',vi.fn(async(path:string)=>{
      if(path.endsWith('/consumer-groups'))return {ok:true,json:async()=>({data:[{id:'billing',state:'Stable',members:3,lag:12,topics:[]}]})};
      await pending;return {ok:true,json:async()=>({data:{id:'billing',state:'Stable',members:3,lag:12,topics:[],offsets:[]}})};
    }));
    mount();fireEvent.click(await screen.findByRole('button',{name:'billing'}));
    expect(await screen.findByRole('progressbar',{name:'Loading committed offsets'})).toBeInTheDocument();
    release(null);
    await waitFor(()=>expect(screen.queryByRole('progressbar')).toBeNull());
  });
});
