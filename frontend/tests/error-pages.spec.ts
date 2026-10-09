import {test,expect} from '@playwright/test';

test.beforeEach(async({page})=>{
  await page.route('**/api/v1/**',route=>{
    const url=new URL(route.request().url());
    if(url.pathname.endsWith('/auth/session'))return route.fulfill({json:{data:{user:{username:'reader',roles:['viewer']},demo:true}}});
    if(url.pathname.endsWith('/clusters'))return route.fulfill({json:{data:[{id:'demo',name:'Demo',topicCount:1}]}});
    return route.fulfill({json:{data:[]}});
  });
});

test('unknown page shows 404 and links back to the console',async({page})=>{
  await page.goto('/clusters/demo/nope');
  await expect(page.getByRole('heading',{name:'Page not found'})).toBeVisible();
  await page.getByRole('link',{name:/Back to console/}).click();
  await expect(page).toHaveURL(/\/$/);
});

test('unknown top-level path shows 404',async({page})=>{
  await page.goto('/nowhere/at/all');
  await expect(page.getByRole('heading',{name:'Page not found'})).toBeVisible();
});

test('unknown cluster id names the missing cluster',async({page})=>{
  await page.goto('/clusters/ghost/topics');
  await expect(page.getByRole('heading',{name:'Cluster not found'})).toBeVisible();
  await expect(page.getByText('No cluster with id “ghost” is configured.')).toBeVisible();
});

test('known pages are not treated as missing',async({page})=>{
  await page.goto('/clusters/demo/overview');
  await expect(page.getByRole('heading',{name:'Page not found'})).toHaveCount(0);
});
