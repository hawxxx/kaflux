import {describe,expect,it} from 'vitest';
import {providerIcon,providerIcons} from './provider-icons';

describe('providerIcon',()=>{
  it('matches configured providers by id or display name',()=>{
    expect(providerIcon('authentik','Authentik')).toBe(providerIcons.authentik);
    expect(providerIcon('corp','Sign in with Okta')).toBe(providerIcons.okta);
    expect(providerIcon('entra','Company login')).toBe(providerIcons.microsoft);
    expect(providerIcon('azure-ad','Azure AD')).toBe(providerIcons.microsoft);
    expect(providerIcon('GitLab','GitLab')).toBe(providerIcons.gitlab);
  });
  it('leaves unknown providers without a brand mark',()=>{
    expect(providerIcon('corp-sso','Corporate SSO')).toBeUndefined();
  });
});
