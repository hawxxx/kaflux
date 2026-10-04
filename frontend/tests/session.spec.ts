import {test,expect} from '@playwright/test';

test('sign out retries failures and returns an authenticated user to login',async({page})=>{
  let attempts=0;
  await page.route('**/api/v1/auth/session',route=>route.fulfill({json:{data:{user:{username:'operator',roles:['administrator']},csrfToken:'browser-csrf',demo:false}}}));
  await page.route('**/api/v1/auth/logout',route=>{
    expect(route.request().method()).toBe('POST');
    expect(route.request().headers()['x-csrf-token']).toBe('browser-csrf');
    attempts++;
    return route.fulfill(attempts===1?{status:503,json:{error:{message:'Session store unavailable'}}}:{json:{data:{ok:true}}});
  });
  await page.goto('/clusters/demo/overview?q=private-topic&sort=name&page=2');
  const button=page.getByRole('button',{name:'Sign out',exact:true});
  await button.scrollIntoViewIfNeeded();await button.click();
  await expect(page.getByRole('alert')).toContainText('Session store unavailable');
  await expect(button).toBeEnabled();await button.click();
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole('heading',{name:'Welcome to your control plane.'})).toBeVisible();
  await expect(page.getByRole('button',{name:'Sign out',exact:true})).toHaveCount(0);
});

test('automatic demo access explains why no sign out action is offered',async({page})=>{
  await page.route('**/api/v1/auth/session',route=>route.fulfill({json:{data:{user:{username:'Demo operator'},csrfToken:'demo',demo:true}}}));
  await page.goto('/clusters/demo/overview');
  await expect(page.getByText('Demo access is automatic. No personal session is active.')).toBeVisible();
  await expect(page.getByRole('button',{name:'Sign out',exact:true})).toHaveCount(0);
});
