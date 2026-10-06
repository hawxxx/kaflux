import {bytes} from './api';

export type ConfigEntry={name:string;value:string|null;source:string;override:boolean;sensitive:boolean};

const LONG_MAX='9223372036854775807';
export const configGroups=['Retention & cleanup','Compaction','Messages','Segments & flushing','Replication','Tiered storage','Other'] as const;
export type ConfigGroup=typeof configGroups[number];

// One-line summaries of the topic-level keys Kafka documents; unknown keys still render without one.
const descriptions:Record<string,string>={
  'cleanup.policy':'Discard old segments (delete), keep the latest value per key (compact), or both.',
  'retention.ms':'How long records are kept before old segments become eligible for deletion.',
  'retention.bytes':'Maximum size a partition can grow to before old segments are deleted.',
  'delete.retention.ms':'How long tombstones are kept for compacted topics.',
  'file.delete.delay.ms':'Delay before a deleted segment file is removed from disk.',
  'min.cleanable.dirty.ratio':'Share of uncompacted log that triggers compaction.',
  'min.compaction.lag.ms':'Minimum time a record stays uncompacted.',
  'max.compaction.lag.ms':'Maximum time a record can remain ineligible for compaction.',
  'max.message.bytes':'Largest record batch the broker accepts for this topic.',
  'compression.type':'Final compression codec; producer keeps the codec the producer used.',
  'compression.gzip.level':'Compression level used when compression.type is gzip.',
  'compression.lz4.level':'Compression level used when compression.type is lz4.',
  'compression.zstd.level':'Compression level used when compression.type is zstd.',
  'message.timestamp.type':'Use the producer timestamp (CreateTime) or the broker append time (LogAppendTime).',
  'message.timestamp.difference.max.ms':'Maximum allowed gap between broker time and record timestamp.',
  'message.timestamp.before.max.ms':'How far in the past a record timestamp may be.',
  'message.timestamp.after.max.ms':'How far in the future a record timestamp may be.',
  'message.downconversion.enable':'Allow down-converting records for older consumers.',
  'message.format.version':'Record format version (deprecated since Kafka 3.0).',
  'segment.bytes':'Size at which the active segment rolls.',
  'segment.ms':'Time after which the active segment rolls even if not full.',
  'segment.jitter.ms':'Random jitter subtracted from segment.ms to avoid simultaneous rolls.',
  'segment.index.bytes':'Size of the offset index for each segment.',
  'index.interval.bytes':'How often an offset index entry is written.',
  'flush.messages':'Force an fsync after this many records.',
  'flush.ms':'Force an fsync after this much time.',
  'preallocate':'Preallocate segment files on disk.',
  'min.insync.replicas':'Replicas that must acknowledge a write when producers use acks=all.',
  'unclean.leader.election.enable':'Allow out-of-sync replicas to become leader, at the risk of data loss.',
  'leader.replication.throttled.replicas':'Replicas throttled on the leader side during reassignment.',
  'follower.replication.throttled.replicas':'Replicas throttled on the follower side during reassignment.',
  'remote.storage.enable':'Offload closed segments to tiered (remote) storage.',
  'local.retention.ms':'How long segments stay on local disk when tiered storage is enabled.',
  'local.retention.bytes':'Local disk size kept per partition when tiered storage is enabled.',
  'remote.log.copy.disable':'Stop copying new segments to remote storage.',
  'remote.log.delete.on.disable':'Delete remote segments when tiered storage is turned off.',
};
export function describeConfig(name:string){return descriptions[name]}

// From the Apache Kafka 3.7–4.1 topic and broker config reference. Every topic-level key is
// dynamic: an override applies live through AlterConfigs, no broker restart. What differs is the
// broker-wide default a key falls back to ("Server Default Property" and its "Update Mode").
type BrokerDefault={property:string;mode:'cluster-wide'|'read-only'};
const brokerDefaults:Record<string,BrokerDefault>={
  'cleanup.policy':{property:'log.cleanup.policy',mode:'cluster-wide'},
  'compression.type':{property:'compression.type',mode:'cluster-wide'},
  'compression.gzip.level':{property:'compression.gzip.level',mode:'cluster-wide'},
  'compression.lz4.level':{property:'compression.lz4.level',mode:'cluster-wide'},
  'compression.zstd.level':{property:'compression.zstd.level',mode:'cluster-wide'},
  'delete.retention.ms':{property:'log.cleaner.delete.retention.ms',mode:'cluster-wide'},
  'file.delete.delay.ms':{property:'log.segment.delete.delay.ms',mode:'cluster-wide'},
  'flush.messages':{property:'log.flush.interval.messages',mode:'cluster-wide'},
  'flush.ms':{property:'log.flush.interval.ms',mode:'cluster-wide'},
  'index.interval.bytes':{property:'log.index.interval.bytes',mode:'cluster-wide'},
  'local.retention.bytes':{property:'log.local.retention.bytes',mode:'cluster-wide'},
  'local.retention.ms':{property:'log.local.retention.ms',mode:'cluster-wide'},
  'max.compaction.lag.ms':{property:'log.cleaner.max.compaction.lag.ms',mode:'cluster-wide'},
  'max.message.bytes':{property:'message.max.bytes',mode:'cluster-wide'},
  'message.downconversion.enable':{property:'log.message.downconversion.enable',mode:'cluster-wide'},
  'message.format.version':{property:'log.message.format.version',mode:'read-only'},
  'message.timestamp.after.max.ms':{property:'log.message.timestamp.after.max.ms',mode:'cluster-wide'},
  'message.timestamp.before.max.ms':{property:'log.message.timestamp.before.max.ms',mode:'cluster-wide'},
  'message.timestamp.difference.max.ms':{property:'log.message.timestamp.difference.max.ms',mode:'cluster-wide'},
  'message.timestamp.type':{property:'log.message.timestamp.type',mode:'cluster-wide'},
  'min.cleanable.dirty.ratio':{property:'log.cleaner.min.cleanable.ratio',mode:'cluster-wide'},
  'min.compaction.lag.ms':{property:'log.cleaner.min.compaction.lag.ms',mode:'cluster-wide'},
  'min.insync.replicas':{property:'min.insync.replicas',mode:'cluster-wide'},
  'preallocate':{property:'log.preallocate',mode:'cluster-wide'},
  'retention.bytes':{property:'log.retention.bytes',mode:'cluster-wide'},
  'retention.ms':{property:'log.retention.ms',mode:'cluster-wide'},
  'segment.bytes':{property:'log.segment.bytes',mode:'cluster-wide'},
  'segment.index.bytes':{property:'log.index.size.max.bytes',mode:'cluster-wide'},
  'segment.jitter.ms':{property:'log.roll.jitter.ms',mode:'cluster-wide'},
  'segment.ms':{property:'log.roll.ms',mode:'cluster-wide'},
  'unclean.leader.election.enable':{property:'unclean.leader.election.enable',mode:'cluster-wide'},
};
const removedIn:Record<string,string>={'message.format.version':'4.0','message.downconversion.enable':'4.0','message.timestamp.difference.max.ms':'4.0'};
export type UpdateMode={label:string;detail:string;kind:'dynamic'|'static'|'topic'};
// How the key's fallback default is maintained, per the Kafka config reference.
export function updateMode(name:string):UpdateMode|undefined{
  const d=brokerDefaults[name];const removed=removedIn[name]?` Removed in Kafka ${removedIn[name]}.`:'';
  if(!d)return descriptions[name]?{label:'Topic only',detail:`No broker-wide default; set per topic, applied live.${removed}`,kind:'topic'}:undefined;
  return d.mode==='read-only'
    ?{label:'Static default',detail:`Broker default ${d.property} is read-only: change it in server.properties and restart brokers. Topic overrides still apply live.${removed}`,kind:'static'}
    :{label:'Dynamic default',detail:`Broker default ${d.property} is dynamic (cluster-wide): changeable without restart. Topic overrides apply live.${removed}`,kind:'dynamic'};
}

export function configGroup(name:string):ConfigGroup{
  if(name.startsWith('remote.')||name.startsWith('local.retention'))return 'Tiered storage';
  if(name.includes('compaction')||name==='min.cleanable.dirty.ratio')return 'Compaction';
  if(name.includes('retention')||name==='cleanup.policy'||name==='file.delete.delay.ms')return 'Retention & cleanup';
  if(name.startsWith('message.')||name.startsWith('compression.')||name==='max.message.bytes')return 'Messages';
  if(name.startsWith('segment.')||name.startsWith('flush.')||name==='index.interval.bytes'||name==='preallocate')return 'Segments & flushing';
  if(name.includes('replica')||name.includes('leader'))return 'Replication';
  return 'Other';
}

const enums:Record<string,string[]>={
  'cleanup.policy':['delete','compact','compact,delete'],
  'compression.type':['producer','uncompressed','gzip','snappy','lz4','zstd'],
  'message.timestamp.type':['CreateTime','LogAppendTime'],
};
export function configOptions(entry:ConfigEntry){return enums[entry.name]??(entry.value==='true'||entry.value==='false'?['true','false']:undefined)}

export function duration(ms:number){
  const units:[number,string][]=[[86_400_000,'day'],[3_600_000,'hour'],[60_000,'minute'],[1000,'second'],[1,'millisecond']];
  const [size,label]=units.find(([size])=>Math.abs(ms)>=size)??units[units.length-1];
  const n=Math.round(ms/size*10)/10;
  return `${n.toLocaleString()} ${label}${n===1?'':'s'}`;
}

// Human reading of a config value, or undefined when the raw value already reads well.
export function humanConfigValue(name:string,value:string|null):string|undefined{
  if(value==null)return 'Hidden';
  if(value==='')return 'None';
  if(value===LONG_MAX)return 'No limit';
  if(value==='true'||value==='false')return value==='true'?'Enabled':'Disabled';
  if(name.startsWith('local.retention')&&value==='-2')return 'Same as retention';
  if((name.startsWith('retention')||name.startsWith('local.retention'))&&value==='-1')return 'Unlimited';
  if(!/^-?\d+(\.\d+)?$/.test(value))return undefined;
  const n=Number(value);
  if(name.endsWith('.ms'))return duration(n);
  if(name.endsWith('.bytes'))return bytes(n);
  if(name==='min.cleanable.dirty.ratio')return `${Math.round(n*100)}%`;
  return undefined;
}
