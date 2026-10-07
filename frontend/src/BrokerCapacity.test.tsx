import {render,screen,within} from '@testing-library/react';
import {describe,expect,it} from 'vitest';
import {CapacityDetail,CapacityTile,capacityHeadline,type BrokerCapacity} from './BrokerCapacity';

const unknown:BrokerCapacity={known:false,status:'unknown',reason:'Capacity is not known for this cluster. Set capacity in the cluster configuration.',busiest:{broker:7,replicas:2310,status:'unknown'},medianReplicas:1980,brokers:[]};
const aws:BrokerCapacity={known:true,source:'aws-msk',label:'express.m7g.4xlarge',reference:'https://docs.aws.amazon.com/msk/latest/developerguide/limits.html',note:'The recommended count is guidance.',status:'ok',partitionsPerBroker:{recommended:6000,maximum:8000},busiest:{broker:3,replicas:2310,percentOfRecommended:38.5,percentOfMaximum:28.9,status:'ok'},medianReplicas:1980,brokers:[]};
const configured:BrokerCapacity={known:true,source:'configured',label:'r6i.4xlarge',status:'above-recommended',partitionsPerBroker:{recommended:4000},diskBytesPerBroker:2*1024**4,busiest:{broker:2,replicas:4800,percentOfRecommended:120,status:'above-recommended'},brokers:[]};

describe('broker capacity',()=>{
  it('says unknown, how to fix it, and still shows the busiest broker and median, when nothing describes the cluster',()=>{
    render(<><CapacityTile capacity={unknown}/><CapacityDetail capacity={unknown}/></>);
    expect(screen.getByText('Broker capacity')).toBeInTheDocument();
    expect(screen.getByText('Unknown')).toBeInTheDocument();
    expect(screen.getByText('No limits set for this cluster')).toBeInTheDocument();
    expect(screen.getByRole('note')).toHaveTextContent('Busiest broker holds 2,310 partition replicas (broker-7), median 1,980.');
    expect(screen.getByRole('note')).toHaveTextContent('Set capacity in the cluster configuration');
  });

  it('treats a response from an older backend, without a capacity object, as unknown',()=>{
    render(<><CapacityTile/><CapacityDetail/></>);
    expect(screen.getByText('Unknown')).toBeInTheDocument();
    expect(screen.queryByRole('note')).toBeNull();
  });

  it('shows the AWS broker type, both limits, the share used, the median and the source with a link',()=>{
    render(<><CapacityTile capacity={aws}/><CapacityDetail capacity={aws}/></>);
    expect(screen.getByText('express.m7g.4xlarge')).toBeInTheDocument();
    expect(screen.getByText('Within limits')).toBeInTheDocument();
    const note=screen.getByRole('note');
    expect(note).toHaveTextContent('6,000 recommended · 8,000 maximum');
    expect(note).toHaveTextContent('Busiest broker (broker-3) holds 2,310 replicas, 39% of the recommended count and 29% of the maximum; median across brokers is 1,980.');
    expect(note).toHaveTextContent('Documented by AWS for this broker type');
    expect(note).toHaveTextContent('The recommended count is guidance.');
    const link=within(note).getByRole('link',{name:'Reference'});
    expect(link).toHaveAttribute('href',aws.reference!);
    expect(link).toHaveAttribute('rel','noreferrer noopener');
    expect(link).toHaveAttribute('target','_blank');
  });

  it('shows operator-configured limits with only a recommended value, disk, and a warning tone when exceeded',()=>{
    render(<><CapacityTile capacity={configured}/><CapacityDetail capacity={configured}/></>);
    expect(screen.getByText('r6i.4xlarge')).toHaveClass('warn-text');
    expect(screen.getByText('Above the recommended count')).toBeInTheDocument();
    const note=screen.getByRole('note');
    expect(note).toHaveTextContent('4,000 recommended.');
    expect(note).not.toHaveTextContent('maximum');
    expect(note).toHaveTextContent('Log storage per broker');
    expect(note).toHaveTextContent('120% of the recommended count');
    expect(note).toHaveTextContent('Configured by your team');
    expect(within(note).queryByRole('link')).toBeNull();
  });

  it('flags a broker over the maximum',()=>{
    const over:BrokerCapacity={...aws,status:'over-maximum'};
    render(<CapacityTile capacity={over}/>);
    expect(screen.getByText('express.m7g.4xlarge')).toHaveClass('warn-text');
    expect(screen.getByText('Over the maximum')).toBeInTheDocument();
  });

  it('offers line breaks inside an instance type without changing its text',()=>{
    const {container}=render(<CapacityTile capacity={aws}/>);
    expect(container.querySelector('strong')).toHaveTextContent('express.m7g.4xlarge');
    expect(container.querySelectorAll('strong wbr').length).toBe(3);
  });

  it('falls back to a generic headline when a known profile has no label',()=>{
    expect(capacityHeadline({...aws,label:undefined})).toBe('Capacity known');
    expect(capacityHeadline(undefined)).toBe('Unknown');
  });

  it('omits the median clause when the backend does not report one',()=>{
    const noMedian:BrokerCapacity={...configured,medianReplicas:undefined};
    render(<CapacityDetail capacity={noMedian}/>);
    expect(screen.getByRole('note')).not.toHaveTextContent('median');
  });
});
