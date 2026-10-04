export type DiffLine={kind:'same'|'added'|'removed';line:string};
export function schemaDiff(before:string,after:string):DiffLine[]{
  const a=before.split('\n'),b=after.split('\n');
  if(a.length>600||b.length>600)throw new Error('Comparison is limited to 600 lines per version. Inspect each version separately for larger schemas.');
  const lengths=Array.from({length:a.length+1},()=>new Uint16Array(b.length+1));
  for(let i=a.length-1;i>=0;i--)for(let j=b.length-1;j>=0;j--)lengths[i][j]=a[i]===b[j]?lengths[i+1][j+1]+1:Math.max(lengths[i+1][j],lengths[i][j+1]);
  const result:DiffLine[]=[];let i=0,j=0;
  while(i<a.length||j<b.length){
    if(i<a.length&&j<b.length&&a[i]===b[j]){result.push({kind:'same',line:a[i]});i++;j++}
    else if(i<a.length&&(j>=b.length||lengths[i+1][j]>=lengths[i][j+1]))result.push({kind:'removed',line:a[i++]});
    else result.push({kind:'added',line:b[j++]});
  }
  return result;
}
