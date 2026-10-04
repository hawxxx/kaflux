import {test,expect} from '@playwright/test';

test.beforeEach(async({page})=>{
  await page.route('**/api/v1/**',route=>{
    const url=new URL(route.request().url());
    if(url.pathname.endsWith('/auth/session'))return route.fulfill({json:{data:{user:{username:'reader',roles:['viewer']},demo:true}}});
    if(url.pathname.endsWith('/clusters'))return route.fulfill({json:{data:[{id:'demo',name:'Demo',topicCount:350}]}});
    if(url.pathname.endsWith('/topics'))return route.fulfill({json:{data:[{name:`orders.${url.searchParams.get('page')??0}`,partitions:3,replicationFactor:3,cleanupPolicy:'delete',sizeBytes:100,urp:0}],meta:{total:350}}});
    return route.fulfill({json:{data:[]}});
  });
});

test('topic table restores shared URL and survives reload and browser history',async({page})=>{
  await page.goto('/clusters/demo/topics?q=orders&sort=name&order=desc&page=1&showSize=false');
  await expect(page.getByLabel('Search topics')).toHaveValue('orders');
  await expect(page.getByText('Page 2',{exact:true})).toBeVisible();
  await expect(page.locator('thead th').filter({hasText:/^Size$/})).toHaveCount(0);
  await expect(page.getByLabel('Select orders.1')).toBeVisible();
  await page.reload();
  await expect(page.getByLabel('Search topics')).toHaveValue('orders');
  await page.getByRole('button',{name:'Next',exact:true}).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.getByText('Page 3',{exact:true})).toBeVisible();
  await page.goBack();
  await expect(page.getByText('Page 2',{exact:true})).toBeVisible();
  await page.goForward();
  await expect(page.getByText('Page 3',{exact:true})).toBeVisible();
  await page.getByLabel('Select orders.2').check();
  await page.getByLabel('Search topics').fill('payments');
  await expect(page.getByText('Page 1',{exact:true})).toBeVisible();
  await expect(page.getByText('1 selected',{exact:true})).toHaveCount(0);
  await page.getByRole('button',{name:'Topic name ↕'}).click();
  await expect(page).toHaveURL(/order=asc/);
  await page.getByRole('button',{name:'Columns',exact:true}).click();
  await page.getByLabel('Storage size').check();
  await expect(page).toHaveURL(/showSize=true/);
  await page.reload();
  await expect(page.locator('thead th').filter({hasText:/^Size$/})).toBeVisible();
  await expect(page.getByLabel('Search topics')).toHaveValue('payments');
});

test('malformed topic state produces bounded API requests',async({page})=>{
  const requests:string[]=[];
  page.on('request',r=>{if(r.url().includes('/topics?'))requests.push(r.url())});
  await page.goto('/clusters/demo/topics?q='+ 'x'.repeat(500)+'&page=-42&sort=unknown&order=sideways&showSize=invalid');
  await expect(page.getByLabel('Search topics')).toHaveValue('x'.repeat(256));
  await expect(page.getByText('Page 1',{exact:true})).toBeVisible();
  await expect(page.locator('thead th').filter({hasText:/^Size$/})).toBeVisible();
  const params=new URL(requests.find(x=>x.includes('pageSize=100'))!).searchParams;
  expect(params.get('q')).toHaveLength(256);
  expect(params.get('page')).toBe('0');
  expect(params.get('sort')).toBe('name');
  expect(params.get('order')).toBe('asc');
});
