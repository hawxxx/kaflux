import {fireEvent,render,screen} from '@testing-library/react';
import {describe,it,expect} from 'vitest';
import {MessageValue} from './MessageValue';
import {hex} from './api';
describe('message inspection',()=>{
  it('collapses structured payload and preserves raw text and binary display',()=>{render(<MessageValue value={'{"event":"created","nested":{"count":3}}'}/>);expect(screen.getByText('"created"')).toBeVisible();fireEvent.click(screen.getByRole('button',{name:/nested/}));expect(screen.queryByText('count:')).not.toBeInTheDocument();fireEvent.click(screen.getByRole('button',{name:'Raw'}));expect(screen.getByText('{"event":"created","nested":{"count":3}}')).toBeVisible();fireEvent.click(screen.getByRole('button',{name:'Hex'}));expect(screen.getByText(hex('{"event":"created","nested":{"count":3}}'))).toBeVisible()});
  it('encodes UTF8 without corrupting unicode',()=>expect(hex('é')).toBe('c3 a9'));
  it('preserves original binary bytes when the server supplies base64',()=>{render(<MessageValue value="unreadable" bytesBase64="AP+A"/>);fireEvent.click(screen.getByRole('button',{name:'Hex'}));expect(screen.getByText('00 ff 80')).toBeVisible()});
});
it('displays actual returned base64 and an explicit unsupported schema decoding state',()=>{
  render(<MessageValue value="preview" bytesBase64="AAEC/w=="/>);
  fireEvent.click(screen.getByRole('button',{name:'Base64'}));
  expect(screen.getByText('AAEC/w==')).toBeVisible();
  fireEvent.click(screen.getByRole('button',{name:'Schema'}));
  expect(screen.getByRole('status')).toHaveTextContent('Schema decoding is unavailable');
});
it('renders decoded Avro with schema identity while preserving original raw bytes',()=>{
 render(<MessageValue value="binary preview" bytesBase64="AAAAACoGYWJj" decodedValue={{name:'abc'}} schemaId={42}/>);
 fireEvent.click(screen.getByRole('button',{name:'Schema'}));
 expect(screen.getByText('Avro · schema 42')).toBeVisible();
 expect(screen.getByText('"abc"')).toBeVisible();
 fireEvent.click(screen.getByRole('button',{name:'Base64'}));
 expect(screen.getByText('AAAAACoGYWJj')).toBeVisible();
});
it('reports per-record decoder failures without hiding raw bytes',()=>{
 render(<MessageValue value="raw" decodeError="Schema subject permission denied"/>);
 fireEvent.click(screen.getByRole('button',{name:'Schema'}));
 expect(screen.getByRole('status')).toHaveTextContent('Schema subject permission denied');
 fireEvent.click(screen.getByRole('button',{name:'Raw'}));
 expect(screen.getByText('raw')).toBeVisible();
});
it('identifies decoded Protobuf and preserves syntax highlighting',()=>{
 render(<MessageValue value="binary" decodedValue={{name:'abc',count:'7'}} schemaId={42} decodedFormat="protobuf"/>);
 fireEvent.click(screen.getByRole('button',{name:'Schema'}));
 expect(screen.getByText('Protobuf · schema 42')).toBeVisible();
 expect(screen.getByText('"abc"')).toHaveClass('json-string');
});
it('pages large decoded collections without rendering every value',()=>{
 const values=Array.from({length:10000},(_,i)=>`record-${i}`);
 const {container}=render(<MessageValue value={values}/>);
 expect(container.querySelectorAll('.json-leaf')).toHaveLength(50);
 expect(screen.getByText('"record-0"')).toBeVisible();
 expect(screen.queryByText('"record-50"')).not.toBeInTheDocument();
 fireEvent.click(screen.getByRole('button',{name:'Next fields'}));
 expect(screen.getByText('"record-50"')).toBeVisible();
 expect(screen.queryByText('"record-0"')).not.toBeInTheDocument();
 expect(container.querySelectorAll('.json-leaf')).toHaveLength(50);
});
