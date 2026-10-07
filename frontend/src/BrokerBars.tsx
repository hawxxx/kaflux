import {useMemo, useState} from 'react';
import type {Broker} from './api';

// A cluster can have dozens of brokers, so the Overview lists them a page at a
// time. The operator can expand the list to see every broker at once.
export const BROKERS_PER_PAGE = 10;

export function BrokerBars({brokers, mode, pageSize = BROKERS_PER_PAGE}: {brokers: Broker[]; mode: string; pageSize?: number}) {
  const [page, setPage] = useState(0);
  const [showAll, setShowAll] = useState(false);
  // Bars scale against the busiest broker in the whole cluster, not the page,
  // so a broker keeps the same bar length while the operator pages.
  const busiest = useMemo(() => brokers.reduce((max, b) => Math.max(max, b.partitions), 1), [brokers]);
  const total = brokers.length;
  const paged = total > pageSize;
  const pages = Math.max(1, Math.ceil(total / pageSize));
  // The list is refreshed in the background and can shrink under the current page.
  const current = Math.min(page, pages - 1);
  const start = paged && !showAll ? current * pageSize : 0;
  const end = paged && !showAll ? Math.min(total, start + pageSize) : total;
  const label = showAll ? `Showing all ${total} brokers` : `Showing ${start + 1}–${end} of ${total} brokers`;
  return <>
    <div className="broker-bars">
      {brokers.slice(start, end).map((b, offset) => <div className="broker-bar-row" key={b.id}>
        <span><i className="good-dot"/>broker-{b.id}<small>{b.rack ?? 'Rack unavailable'}</small></span>
        <div className="bar-track"><div style={{width: `${Math.max(2, b.partitions / busiest * 100)}%`, background: `var(--broker-${(start + offset) % 3})`}}/></div>
        <strong>{b.partitions}<small>partitions</small></strong>
      </div>)}
    </div>
    {paged && <div className="broker-pager">
      <span role="status">{label}</span>
      <div>
        {!showAll && <>
          <button className="button" aria-label="Previous brokers" disabled={current === 0} onClick={() => setPage(current - 1)}>Previous</button>
          <span className="broker-page-count">Page {current + 1} of {pages}</span>
          <button className="button" aria-label="Next brokers" disabled={current >= pages - 1} onClick={() => setPage(current + 1)}>Next</button>
        </>}
        <button className="button" aria-expanded={showAll} onClick={() => {setShowAll(!showAll); setPage(0)}}>{showAll ? 'Show fewer' : `Show all ${total}`}</button>
      </div>
    </div>}
    <div className="panel-bottom">{total} brokers · {mode}<span>Inventory source: Kafka Admin API</span></div>
  </>;
}
