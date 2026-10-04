export type Cluster = {id:string;name:string;environment:string;kind:string;mode:string;state:string;brokerCount:number;topicCount:number;partitionCount:number};
export type Topic = {name:string;partitions:number;replicationFactor:number;sizeBytes:number|null;urp:number;cleanupPolicy:string;retentionMs:number;observedAt:string};
export type Broker = {id:number;host:string;port:number;rack:string;partitions:number;leaders:number;sizeBytes:number|null};
export type Message = {partition:number;offset:number;timestamp:string;key:unknown;value:unknown;headers:{key:string;value:string}[];valueBase64?:string;keyBase64?:string;truncated?:boolean;decodedValue?:unknown;schemaId?:number;decodeError?:string;decodedFormat?:string;decodedKey?:unknown;keyDecodedFormat?:string;keySchemaId?:number;keyDecodeError?:string};
export type Session = {user:{username:string;name?:string;role?:string;roles?:string[];permissions?:string[]};csrfToken:string;demo:boolean;canManageSessions?:boolean};
export type Envelope<T> = {data:T;meta?:{total:number;page:number;pageSize:number}};
export class ApiError extends Error {code:string;status:number;constructor(message:string,code:string,status:number){super(message);this.code=code;this.status=status}}
let csrf='';
export function setCsrf(value:string){csrf=value}
export async function api<T>(path:string,options:RequestInit={}):Promise<Envelope<T>> {
  const response=await fetch(`/api/v1${path}`,{credentials:'include',...options,headers:{'Content-Type':'application/json',...(options.method&&options.method!=='GET'?{'X-CSRF-Token':csrf}:{}),...options.headers}});
  const body=await response.json().catch(()=>null);
  if(!response.ok)throw new ApiError(body?.error?.message??`Request failed (${response.status})`,body?.error?.code??'REQUEST_FAILED',response.status);
  return body;
}
export function pretty(value:unknown){if(typeof value==='string'){try{return JSON.stringify(JSON.parse(value),null,2)}catch{return value}}return JSON.stringify(value,null,2)??'null'}
export function asJson(value:unknown):unknown{if(typeof value==='string'){try{return JSON.parse(value)}catch{return value}}return value}
export function hex(value:unknown){return Array.from(new TextEncoder().encode(typeof value==='string'?value:JSON.stringify(value)??'')).map(x=>x.toString(16).padStart(2,'0')).join(' ')}
export function bytes(value:number|null|undefined){if(value==null)return 'Unavailable';if(value<1024)return `${Number.isInteger(value)?value:value.toFixed(1)} B`;const n=Math.min(Math.floor(Math.log(value)/Math.log(1024)),4);return `${(value/1024**n).toFixed(1)} ${['B','KiB','MiB','GiB','TiB'][n]}`}
const compact=new Intl.NumberFormat(undefined,{notation:'compact',maximumFractionDigits:1});
// Units follow the Grafana unit identifiers used by the metric catalog.
export function metricValue(value:number,unit=''){switch(unit){case 'Bps':return `${bytes(value)}/s`;case 'decbytes':return bytes(value);case 'bps':return `${compact.format(value)} b/s`;case 'reqps':return `${compact.format(value)}/s`;case 'percent':return `${compact.format(value)}%`;case 'percentunit':return `${compact.format(value*100)}%`;case 'ms':case 'ns':case 's':return `${compact.format(value)} ${unit}`;default:return compact.format(value)}}
