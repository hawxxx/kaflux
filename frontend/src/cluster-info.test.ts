import {describe,expect,it} from 'vitest';
import {kafkaSummary,metadataModeLabel} from './api';

describe('kafkaSummary',()=>{
  it('names version and metadata mode',()=>{
    expect(kafkaSummary({kafkaVersion:'3.7',metadataMode:'kraft'})).toBe('Kafka 3.7 · KRaft');
    expect(kafkaSummary({kafkaVersion:'2.8',metadataMode:'zookeeper'})).toBe('Kafka 2.8 · ZooKeeper');
  });
  it('omits what brokers did not report',()=>{
    expect(kafkaSummary({kafkaVersion:'3.6'})).toBe('Kafka 3.6');
    expect(kafkaSummary({})).toBe('');
    expect(kafkaSummary(undefined)).toBe('');
    expect(metadataModeLabel('other')).toBeUndefined();
  });
});
