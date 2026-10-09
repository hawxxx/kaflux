import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {Integrations} from './Integrations';
import {allPermissions} from './test-permissions';
afterEach(()=>vi.restoreAllMocks());
const session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:false};
function mount(kind:'schemas'|'connectors'){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><Integrations clusterId="demo" kind={kind} session={session}/></QueryClientProvider>)}
it('explains unconfigured schema integration without synthetic subjects',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>({ok:false,status:422,json:async()=>({error:{code:'integration_unconfigured',message:'Schema Registry integration not configured'}})} as Response)));
  mount('schemas');expect(await screen.findByRole('alert')).toHaveTextContent('Schema Registry integration not configured');expect(screen.queryByRole('button',{name:'Register schema'})).not.toBeInTheDocument();
});
it('requires approval before creating an actual connector',async()=>{
  vi.stubGlobal('fetch',vi.fn(async(path:string)=>({ok:true,json:async()=>({data:path.endsWith('/connectors')?[{id:'connect-main',kind:'connect'}]:[]})} as Response)));
  mount('connectors');fireEvent.click(await screen.findByRole('button',{name:'Create connector'}));expect(screen.getByRole('button',{name:'Confirm operation'})).toBeDisabled();expect(screen.getByText(/server-managed integration/)).toBeVisible();
});
it('preloads redacted connector configuration and submits only after review',async()=>{
  const writes:any[]=[];
  vi.stubGlobal('fetch',vi.fn(async(path:string,options?:RequestInit)=>{
    if(options?.body)writes.push(JSON.parse(String(options.body)));
    return {ok:true,json:async()=>({data:path.endsWith('/connectors')?[{id:'connect-main',kind:'connect'}]:path.endsWith('/connect-main')?['sink']:path.endsWith('/config')?{'connector.class':'example.Sink','sasl.password':'[REDACTED]'}:{tasks:[]}})} as Response;
  }));
  mount('connectors');fireEvent.click(await screen.findByRole('button',{name:'sink'}));fireEvent.click(await screen.findByRole('button',{name:'Update connector configuration'}));
  await waitFor(()=>expect(screen.getByRole('textbox',{name:'Complete intended connector configuration · JSON'})).toHaveValue(JSON.stringify({'connector.class':'example.Sink','sasl.password':'[REDACTED]'},null,2)));
  fireEvent.click(screen.getByLabelText('I reviewed the resource and operation.'));fireEvent.click(screen.getByRole('button',{name:'Confirm operation'}));
  await waitFor(()=>expect(writes).toEqual([{confirmation:true,payload:{'connector.class':'example.Sink','sasl.password':'[REDACTED]'}}]));
});
