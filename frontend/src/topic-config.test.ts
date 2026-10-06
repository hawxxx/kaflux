import {expect,it} from 'vitest';
import {configGroup,configOptions,humanConfigValue,updateMode} from './topic-config';
it('groups topic configs by concern',()=>{
  expect(configGroup('retention.ms')).toBe('Retention & cleanup');
  expect(configGroup('max.compaction.lag.ms')).toBe('Compaction');
  expect(configGroup('compression.type')).toBe('Messages');
  expect(configGroup('segment.bytes')).toBe('Segments & flushing');
  expect(configGroup('min.insync.replicas')).toBe('Replication');
  expect(configGroup('local.retention.ms')).toBe('Tiered storage');
  expect(configGroup('something.new')).toBe('Other');
});
it('reads config values in human units',()=>{
  expect(humanConfigValue('retention.ms','604800000')).toBe('7 days');
  expect(humanConfigValue('retention.ms','-1')).toBe('Unlimited');
  expect(humanConfigValue('segment.bytes','1073741824')).toBe('1.0 GiB');
  expect(humanConfigValue('flush.ms','9223372036854775807')).toBe('No limit');
  expect(humanConfigValue('local.retention.bytes','-2')).toBe('Same as retention');
  expect(humanConfigValue('preallocate','false')).toBe('Disabled');
  expect(humanConfigValue('min.cleanable.dirty.ratio','0.5')).toBe('50%');
  expect(humanConfigValue('compression.type','zstd')).toBeUndefined();
});
it('offers choices for enum and boolean configs',()=>{
  expect(configOptions({name:'cleanup.policy',value:'delete',source:'',override:false,sensitive:false})).toContain('compact,delete');
  expect(configOptions({name:'preallocate',value:'false',source:'',override:false,sensitive:false})).toEqual(['true','false']);
  expect(configOptions({name:'segment.ms',value:'1',source:'',override:false,sensitive:false})).toBeUndefined();
});
it('reports static or dynamic defaults from the Kafka reference',()=>{
  expect(updateMode('retention.ms')).toMatchObject({label:'Dynamic default',kind:'dynamic'});
  expect(updateMode('retention.ms')?.detail).toContain('log.retention.ms');
  expect(updateMode('message.format.version')).toMatchObject({label:'Static default',kind:'static'});
  expect(updateMode('message.format.version')?.detail).toContain('Removed in Kafka 4.0');
  expect(updateMode('leader.replication.throttled.replicas')).toMatchObject({label:'Topic only',kind:'topic'});
  expect(updateMode('vendor.custom')).toBeUndefined();
});
