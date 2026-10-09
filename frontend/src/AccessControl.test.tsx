import {fireEvent,render,screen,waitFor} from '@testing-library/react';
import {QueryClient,QueryClientProvider} from '@tanstack/react-query';
import {afterEach,expect,it,vi} from 'vitest';
import {AccessControl} from './AccessControl';
import {allPermissions} from './test-permissions';
const acl={resourceType:'TOPIC',resourceName:'orders',patternType:'PREFIXED',principal:'User:orders',host:'*',operation:'READ',permission:'ALLOW',raw:{resourceType:2,patternType:4,operation:3,permission:3}};
const session={user:{username:'admin',role:'administrator'},permissions:allPermissions('demo'),csrfToken:'test',demo:true};
function mount(){render(<QueryClientProvider client={new QueryClient({defaultOptions:{queries:{retry:false}}})}><AccessControl clusterId="demo" session={session}/></QueryClientProvider>)}
afterEach(()=>vi.restoreAllMocks());
it('requires reviewed approval and deletes only the exact selected binding',async()=>{
  const requests:any[]=[];vi.stubGlobal('fetch',vi.fn(async(_path:string,options?:RequestInit)=>{if(options?.body)requests.push({method:options.method,body:JSON.parse(String(options.body))});return {ok:true,json:async()=>({data:options?.method?{deleted:1}:[acl]})} as Response}));
  mount();await screen.findByText('User:orders');fireEvent.click(screen.getByRole('button',{name:'Delete ACL'}));expect(screen.getByRole('button',{name:'Confirm deletion'})).toBeDisabled();fireEvent.click(screen.getByLabelText('I reviewed this exact ACL binding.'));fireEvent.click(screen.getByRole('button',{name:'Confirm deletion'}));await waitFor(()=>expect(requests).toEqual([{method:'DELETE',body:{acl:{resourceType:'TOPIC',resourceName:'orders',patternType:'PREFIXED',principal:'User:orders',host:'*',operation:'READ',permission:'ALLOW'},confirmation:true}}]));
});
it('shows authorizer unsupported state without invented ACL entries',async()=>{
  vi.stubGlobal('fetch',vi.fn(async()=>({ok:false,status:422,json:async()=>({error:{code:'acl_unsupported',message:'Kafka authorizer not enabled'}})} as Response)));
  mount();expect(await screen.findByRole('alert')).toHaveTextContent('Kafka authorizer not enabled');expect(screen.getByRole('button',{name:'Create ACL'})).toBeDisabled();
});
