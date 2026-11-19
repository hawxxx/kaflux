import {useEffect,useRef} from 'react';

export type BackdropSignal='input'|'submit'|'error';
const signalEvent='kaflux:login-backdrop';

/** Lets the sign-in form nudge the orb: typing charges it, submit swirls it, failure flushes it red. */
export function signalBackdrop(kind:BackdropSignal){window.dispatchEvent(new CustomEvent(signalEvent,{detail:kind}))}

type Particle={lat:number;lon:number;shell:number;phase:number;big:boolean;ox:number;oy:number;delay:number;sx:number;sy:number;sz:number};
type Bokeh={x:number;y:number;r:number;z:number;sprite:HTMLCanvasElement;v:number};
type Ripple={x:number;y:number;t0:number};

function sprite(r:number,g:number,b:number){
  const c=document.createElement('canvas');c.width=c.height=128;
  const x=c.getContext('2d');if(!x)return c;
  const gr=x.createRadialGradient(64,64,0,64,64,64);
  gr.addColorStop(0,`rgba(${r},${g},${b},1)`);gr.addColorStop(.5,`rgba(${r},${g},${b},.4)`);gr.addColorStop(1,`rgba(${r},${g},${b},0)`);
  x.fillStyle=gr;x.fillRect(0,0,128,128);return c;
}

const ease=(u:number)=>u<=0?0:u>=1?1:1-Math.pow(1-u,4);

/** Animated particle orb behind the sign-in card. Purely decorative and hidden from assistive technology. */
export function LoginBackdrop(){
  const main=useRef<HTMLCanvasElement>(null),glow=useRef<HTMLCanvasElement>(null);
  useEffect(()=>{
    const c=main.current,b=glow.current;if(!c||!b)return;
    const x=c.getContext('2d'),bx=b.getContext('2d');if(!x||!bx)return;
    const reduce=window.matchMedia?.('(prefers-reduced-motion: reduce)').matches??false;
    const dpr=Math.min(window.devicePixelRatio||1,2);
    const count=window.innerWidth<700?4000:9000;
    const particles:Particle[]=[];
    for(let i=0;i<count;i++){
      const a=Math.random()*6.28,r=1.6+Math.random()*2.2;
      particles.push({lat:Math.asin(Math.random()*2-1),lon:Math.random()*Math.PI*2,shell:.82+Math.pow(Math.random(),3.4)*.55,phase:Math.random()*6.28,big:Math.random()<.05,ox:0,oy:0,delay:Math.random()*.9,sx:Math.cos(a)*r,sy:Math.sin(a)*r*.7,sz:(Math.random()-.5)*2});
    }
    const sprites=[sprite(150,120,255),sprite(100,150,255),sprite(200,130,240)];
    const bokeh:Bokeh[]=Array.from({length:22},(_,i)=>({x:Math.random(),y:Math.random(),r:30+Math.random()*80,z:Math.random(),sprite:sprites[i%3],v:(Math.random()-.5)*.006}));
    const ripples:Ripple[]=[];
    let W=1,H=1,t=0,energy=0,swirl=0,errorAt=-9,mx=-1e4,my=-1e4,nx=.5,ny=.5,raf=0,last=performance.now();
    const resize=()=>{const r=c.getBoundingClientRect();W=c.width=Math.max(1,Math.round(r.width*dpr));H=c.height=Math.max(1,Math.round(r.height*dpr));b.width=Math.max(1,W>>2);b.height=Math.max(1,H>>2)};
    const onMove=(e:PointerEvent)=>{mx=e.clientX*dpr;my=e.clientY*dpr;nx=e.clientX/window.innerWidth;ny=1-e.clientY/window.innerHeight};
    const onDown=(e:PointerEvent)=>{if((e.target as Element|null)?.closest('.login-card'))return;ripples.push({x:e.clientX*dpr,y:e.clientY*dpr,t0:t})};
    const onSignal=(e:Event)=>{const kind=(e as CustomEvent<BackdropSignal>).detail;if(kind==='input')energy=Math.min(1,energy+.12);else if(kind==='submit'){energy=1;swirl=1}else errorAt=t};
    function frame(dt:number){
      if(!x||!bx||!c||!b)return;
      t+=dt;energy*=Math.pow(.35,dt);swirl*=Math.pow(.25,dt);
      const cx=W*.5,cy=H*.5,R=Math.min(W,H)*.46*(1+energy*.035+Math.sin(t*.8)*.008);
      const er=Math.max(0,1-(t-errorAt)/1.4);
      x.globalCompositeOperation='source-over';x.globalAlpha=1;x.fillStyle='#06060a';x.fillRect(0,0,W,H);
      x.globalCompositeOperation='lighter';
      for(const o of bokeh){
        o.y+=o.v*dt*(1+o.z);if(o.y<-.2)o.y=1.2;if(o.y>1.2)o.y=-.2;
        const px=(o.x+(nx-.5)*.04*(o.z+.3))*W,py=(o.y-(ny-.5)*.04*(o.z+.3))*H,r=o.r*dpr*(.6+o.z);
        x.globalAlpha=.03+o.z*.04;x.drawImage(o.sprite,px-r,py-r,r*2,r*2);
      }
      x.globalAlpha=1;
      const halo=x.createRadialGradient(cx,cy,0,cx,cy,R*1.9);
      halo.addColorStop(0,er>0?`rgba(${120+er*110},${80-er*30},${230-er*150},.26)`:'rgba(118,84,235,.24)');
      halo.addColorStop(.38,'rgba(58,96,215,.09)');halo.addColorStop(1,'rgba(0,0,0,0)');
      x.fillStyle=halo;x.fillRect(0,0,W,H);
      const core=x.createRadialGradient(cx,cy,0,cx,cy,R*.75);
      core.addColorStop(0,`rgba(190,170,255,${.07+energy*.12})`);core.addColorStop(1,'rgba(0,0,0,0)');
      x.fillStyle=core;x.fillRect(cx-R,cy-R,R*2,R*2);
      const ry=t*.06+(nx-.5)*.7,rx=.38+Math.sin(t*.05)*.1+(ny-.5)*.45;
      const cr=Math.cos(ry),sr=Math.sin(ry),cxr=Math.cos(rx),sxr=Math.sin(rx),reach=(160*dpr)**2;
      for(let i=ripples.length-1;i>=0;i--)if(t-ripples[i].t0>2.4)ripples.splice(i,1);
      for(const p of particles){
        // Latitudes drift at different speeds so bands appear to flow across the surface.
        const lon=p.lon+t*(.05+.07*Math.sin(p.lat*3+1.3))+swirl*.9*Math.cos(p.lat);
        const cl=Math.cos(p.lat);
        const wave=.045*Math.sin(p.lat*6+t*.9+p.phase*.15)*Math.cos(lon*3-t*.6)+energy*.06*Math.sin(p.lat*11+lon*4-t*6);
        const s=p.shell*(1+wave);
        const X=cl*Math.cos(lon)*s,Y=Math.sin(p.lat)*s,Z=cl*Math.sin(lon)*s;
        const x1=X*cr-Z*sr,z1=X*sr+Z*cr,y1=Y*cxr-z1*sxr,z2=Y*sxr+z1*cxr;
        const k=ease((t-p.delay)/1.8);
        const vx=x1*k+p.sx*(1-k),vy=y1*k+p.sy*(1-k),vz=z2*k+p.sz*(1-k);
        const f=2.6/(2.6+vz);
        let px=cx+vx*R*f,py=cy+vy*R*f;
        for(const r of ripples){
          const age=t-r.t0,dx=px-r.x,dy=py-r.y,d=Math.sqrt(dx*dx+dy*dy)/dpr;
          const push=Math.exp(-Math.pow((d-age*520)/40,2))*Math.exp(-age*1.4)*18*dpr;
          if(push>.01){px+=dx/(d*dpr+1e-3)*push;py+=dy/(d*dpr+1e-3)*push}
        }
        const dx=px+p.ox-mx,dy=py+p.oy-my,d2=dx*dx+dy*dy;
        if(d2<reach){const kk=(1-d2/reach)*10*dpr/Math.sqrt(d2+1);p.ox+=dx*kk;p.oy+=dy*kk}
        p.ox*=.93;p.oy*=.93;px+=p.ox;py+=p.oy;
        const depth=(1-vz)*.5;
        const hue=(258+34*Math.sin(p.lat*2.2+lon*.9+t*.18)-18*Math.cos(p.lat*5-t*.11))*(1-er)+352*er;
        const twinkle=.85+.15*Math.sin(t*2.3+p.phase*7);
        x.fillStyle=`hsla(${hue},92%,${60+depth*24+energy*8}%,${(.12+depth*.8)*twinkle*(p.big?1:.85)*(.3+.7*k)})`;
        const size=(p.big?2:1)*(.7+depth*1.5)*dpr;
        x.fillRect(px,py,size,size);
      }
      for(let i=0;i<6;i++){
        const o=bokeh[i],px=((o.x*1.3+t*.006*(i+1))%1.4-.2)*W+(nx-.5)*60*dpr,py=o.y*H-(ny-.5)*40*dpr,r=(110+o.r)*dpr;
        x.globalAlpha=.02;x.drawImage(o.sprite,px-r,py-r,r*2,r*2);
      }
      x.globalAlpha=1;
      bx.clearRect(0,0,b.width,b.height);bx.drawImage(c,0,0,b.width,b.height);
    }
    const loop=(now:number)=>{const dt=Math.min(.05,(now-last)/1000);last=now;if(!document.hidden)frame(dt);raf=requestAnimationFrame(loop)};
    resize();
    const observer=typeof ResizeObserver==='undefined'?undefined:new ResizeObserver(()=>{resize();if(reduce)frame(0)});
    observer?.observe(c);
    window.addEventListener(signalEvent,onSignal);
    if(reduce){t=3;frame(0)}
    else{window.addEventListener('pointermove',onMove);window.addEventListener('pointerdown',onDown);raf=requestAnimationFrame(loop)}
    return ()=>{cancelAnimationFrame(raf);observer?.disconnect();window.removeEventListener(signalEvent,onSignal);window.removeEventListener('pointermove',onMove);window.removeEventListener('pointerdown',onDown)};
  },[]);
  return <div className="login-backdrop" aria-hidden="true"><canvas ref={main}/><canvas ref={glow} className="login-bloom"/><div className="login-grain"/></div>;
}
