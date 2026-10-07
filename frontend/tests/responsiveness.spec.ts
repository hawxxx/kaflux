import {test,expect} from '@playwright/test';

test.beforeEach(async({page})=>{
  await page.route('**/api/v1/**',route=>{
    const path=new URL(route.request().url()).pathname;
    if(path.endsWith('/auth/session'))return route.fulfill({json:{data:{user:{username:'operator',roles:['administrator']},demo:true,csrfToken:'demo-csrf'}}});
    if(path.endsWith('/clusters'))return route.fulfill({json:{data:[{id:'demo',name:'Development simulator',environment:'development',mode:'demo',state:'healthy',topicCount:6}]}});
    if(path.endsWith('/overview'))return route.fulfill({json:{data:{cluster:{mode:'demo'},brokers:[],observedAt:'2026-10-04T00:00:00Z',totals:{brokers:6,topics:6,partitions:72,dataSize:40480000000,consumerLag:51,urp:0,offline:0}}}});
    return route.fulfill({json:{data:[]}});
  });
});

test('phone header fits narrow screens and exposes usable named controls',async({page})=>{
  for(const width of [320,390]){
    await page.setViewportSize({width,height:844});
    await page.goto('/clusters/demo/reassignments');
    await expect(page.getByRole('heading',{name:'Reassignments',exact:true})).toBeVisible();
    expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
    for(const name of ['Toggle navigation','Search anything','Toggle light theme']){
      const control=page.getByRole('button',{name,exact:true});
      await expect(control).toBeVisible();
      const box=await control.boundingBox();
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
      expect(box!.x+box!.width).toBeLessThanOrEqual(width);
    }
  }
});

test('mobile drawer traps focus, dismisses and keeps hidden navigation out of tab order',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto('/clusters/demo/overview');
  const trigger=page.getByRole('button',{name:'Toggle navigation'});
  await trigger.click();
  const drawer=page.getByRole('dialog',{name:'Workspace navigation'});
  await expect(drawer).toBeVisible();
  await expect(trigger).toHaveAttribute('aria-expanded','true');
  for(let i=0;i<24;i++){
    await page.keyboard.press('Tab');
    expect(await page.evaluate(()=>!!document.activeElement?.closest('.sidebar'))).toBe(true);
  }
  await page.keyboard.press('Escape');
  await expect(drawer).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await expect(trigger).toHaveAttribute('aria-expanded','false');
  expect(await page.locator('.sidebar').evaluate(e=>e.hasAttribute('inert'))).toBe(true);
  await page.keyboard.press('Tab');
  await expect(page.getByRole('button',{name:'Search anything',exact:true})).toBeFocused();
  await trigger.click();
  await drawer.getByRole('button',{name:'Messages',exact:true}).click();
  await expect(page.getByRole('heading',{name:'Messages',exact:true})).toBeVisible();
  await expect(drawer).toHaveCount(0);
  await trigger.click();
  await page.locator('.navigation-backdrop').click({position:{x:350,y:100}});
  await expect(drawer).toHaveCount(0);
  await expect(trigger).toBeFocused();
  await trigger.click();
  await page.keyboard.press('Control+k');
  await expect(drawer).toHaveCount(0);
  await expect(page.getByRole('dialog',{name:'Search your workspace'})).toBeVisible();
  await expect(page.getByPlaceholder('Search topics or jump to a page…')).toBeFocused();
  await page.keyboard.press('Escape');
  await trigger.click();
  await page.setViewportSize({width:1024,height:900});
  await expect(drawer).toHaveCount(0);
  expect(await page.locator('.sidebar').evaluate(e=>e.hasAttribute('inert'))).toBe(false);
  expect(await page.locator('.main-shell').evaluate(e=>e.hasAttribute('inert'))).toBe(false);
});

test('tablet overview leaves two usable statistic columns',async({page})=>{
  await page.setViewportSize({width:768,height:900});
  await page.goto('/clusters/demo/overview');
  await expect(page.locator('.stat-card').first()).toBeVisible();
  const columns=await page.locator('.stat-grid').evaluate(e=>getComputedStyle(e).gridTemplateColumns.split(' ').length);
  expect(columns).toBe(2);
});

test('phone loading and failure states remain usable and refresh recovers',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  let release!:()=>void;
  const pending=new Promise<void>(resolve=>{release=resolve});
  let unavailable=true;
  await page.route('**/api/v1/clusters/demo/topics?**',async route=>{
    if(unavailable){
      await pending;
      return route.fulfill({status:503,json:{error:{code:'broker_unavailable',message:'Metadata unavailable'}}});
    }
    return route.fulfill({json:{data:[],meta:{total:0}}});
  });
  await page.goto('/clusters/demo/topics');
  await expect(page.getByText('Loading topics…')).toBeVisible();
  release();
  await expect(page.getByRole('alert')).toContainText('Metadata unavailable');
  await expect(page.getByRole('button',{name:'Refresh',exact:true})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
  unavailable=false;
  await page.getByRole('button',{name:'Refresh',exact:true}).click();
  await expect(page.getByRole('alert')).toHaveCount(0);
  await expect(page.getByText('No records match this view.')).toBeVisible();
});

test('tablet header keeps the current page name whole and the logo visible',async({page})=>{
  for(const width of [768,1024]){
    await page.setViewportSize({width,height:900});
    await page.goto('/clusters/demo/consumer-groups');
    const current=page.locator('.breadcrumb>strong');
    await expect(current).toHaveText('Consumer groups');
    expect(await current.evaluate(e=>e.scrollWidth<=e.clientWidth)).toBe(true);
    expect((await page.locator('.breadcrumb').boundingBox())!.height).toBeLessThanOrEqual(20);
    expect((await page.locator('.brand svg').boundingBox())!.width).toBeGreaterThanOrEqual(24);
  }
});
