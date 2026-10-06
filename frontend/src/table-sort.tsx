import {useState,type ReactNode} from 'react';
import {ArrowDown,ArrowUp,ArrowUpDown} from 'lucide-react';

export type SortDirection='asc'|'desc';
export type SortState<K extends string>={key:K;direction:SortDirection}|null;

/** Missing values sort below everything; numbers compare numerically and strings naturally ("p2" < "p10"). */
export function compareValues(a:unknown,b:unknown):number{
  const missingA=a==null||a==='',missingB=b==null||b==='';
  if(missingA||missingB)return missingA===missingB?0:missingA?-1:1;
  if(typeof a==='number'&&typeof b==='number')return a-b;
  if(typeof a==='boolean'&&typeof b==='boolean')return Number(a)-Number(b);
  const text=(v:unknown)=>typeof v==='object'?JSON.stringify(v):String(v);
  return text(a).localeCompare(text(b),undefined,{numeric:true,sensitivity:'base'});
}

/** Client-side sorting for plain tables: first click ascending, second descending, third restores source order. */
export function useSortedRows<T,K extends string>(rows:T[],accessors:Record<K,(row:T)=>unknown>,initial:SortState<K>=null){
  const [sort,setSort]=useState<SortState<K>>(initial);
  const sorted=sort?rows.map((row,index)=>({row,index})).sort((x,y)=>{const c=compareValues(accessors[sort.key](x.row),accessors[sort.key](y.row));return (sort.direction==='desc'?-c:c)||x.index-y.index}).map(x=>x.row):rows;
  const toggle=(key:K)=>setSort(old=>old?.key!==key?{key,direction:'asc'}:old.direction==='asc'?{key,direction:'desc'}:null);
  const direction=(key:K)=>sort?.key===key?sort.direction:false;
  return {sorted,sort,toggle,direction};
}

export function ariaSort(direction:SortDirection|false){return direction==='asc'?'ascending':direction==='desc'?'descending':'none'}

/** `pressed` exposes state when the button is not inside a header cell carrying aria-sort. */
export function SortButton({children,direction,onClick,pressed}:{children:ReactNode;direction:SortDirection|false;onClick:()=>void;pressed?:boolean}){
  const Icon=direction==='asc'?ArrowUp:direction==='desc'?ArrowDown:ArrowUpDown;
  return <button type="button" className={`sort-button${direction?' sorted':''}`} onClick={onClick} aria-pressed={pressed}>{children}<Icon size={11} aria-hidden="true"/></button>;
}

/** Header cell for plain tables backed by useSortedRows. */
export function SortTh<K extends string>({label,sortKey,table}:{label:string;sortKey:K;table:{toggle:(key:K)=>void;direction:(key:K)=>SortDirection|false}}){
  const direction=table.direction(sortKey);
  return <th scope="col" aria-sort={ariaSort(direction)}><SortButton direction={direction} onClick={()=>table.toggle(sortKey)}>{label}</SortButton></th>;
}
