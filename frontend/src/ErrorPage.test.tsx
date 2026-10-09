import {render,screen} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {ErrorPage} from './ErrorPage';

describe('ErrorPage',()=>{
  it('explains a missing page and links home without a reload button',()=>{
    render(<ErrorPage code={404}/>);
    expect(screen.getByRole('heading',{name:'Page not found'})).toBeInTheDocument();
    expect(screen.getByRole('link',{name:/Back to console/})).toHaveAttribute('href','/');
    expect(screen.queryByRole('button',{name:'Reload'})).toBeNull();
  });
  it('announces server errors, shows detail and offers reload',()=>{
    render(<ErrorPage code={500} detail="boom"/>);
    expect(screen.getByRole('alert')).toHaveTextContent('Something went wrong');
    expect(screen.getByText('boom')).toBeInTheDocument();
    expect(screen.getByRole('button',{name:'Reload'})).toBeInTheDocument();
  });
});
