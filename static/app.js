const DISPLAY_LIMIT = 260;
const LOOKBACK_DAYS = 5475;
const BIG_MOVER_THRESHOLD = 5;

const tableBody = document.getElementById('table-body');
const asOfLabel = document.getElementById('as-of');
const latestTag = document.getElementById('latest-tag');
const selectedDateLabel = document.getElementById('selected-date');
const topTotalLabel = document.getElementById('top-total');
const entrantsList = document.getElementById('entrants');
const moversList = document.getElementById('movers');
const flowSelectedLabel = document.getElementById('flow-selected');
const detailSelectedLabel = document.getElementById('detail-selected');
const chartRankCapSelect = document.getElementById('chart-rank-cap');
const symbolInput = document.getElementById('symbol-input');
const focusSymbolButton = document.getElementById('focus-symbol');
const eventDaysSelect = document.getElementById('event-days');
const eventMaxItemsSelect = document.getElementById('event-max-items');

let flowChart = null;
let detailChart = null;
let timeline = null;
let activeSymbol = null;
let currentDateIndex = 0;
let seriesBySymbol = new Map();
let historyBySymbol = new Map();
let historyAbortController = null;
let timelineAbortController = null;

const fmtCap = (num) => {
  if (num === null || num === undefined || Number.isNaN(num)) return '—';
  if (num >= 1e12) return `${(num / 1e12).toFixed(2)}조`;
  if (num >= 1e8) return `${(num / 1e8).toFixed(0)}억`;
  return num.toLocaleString();
};

const fmtPct = (num) => {
  if (num === null || num === undefined || Number.isNaN(num)) return '—';
  const sign = num > 0 ? '+' : '';
  return `${sign}${num.toFixed(2)}%`;
};

const fmtSignedCap = (num) => {
  if (num === null || num === undefined || Number.isNaN(num)) return '—';
  const sign = num > 0 ? '+' : '';
  return `${sign}${fmtCap(Math.abs(num))}`;
};

const symbolColor = (symbol, alpha = 1) => {
  let hash = 0;
  for (let i = 0; i < symbol.length; i += 1) hash = (hash * 31 + symbol.charCodeAt(i)) % 360;
  return `hsla(${hash}, 80%, 60%, ${alpha})`;
};

function clearChildren(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function setTableMessage(message) {
  clearChildren(tableBody);
  const tr = document.createElement('tr');
  const td = document.createElement('td');
  td.colSpan = 3;
  td.textContent = message;
  tr.appendChild(td);
  tableBody.appendChild(tr);
}

function createEventItem(message, tagText, symbol) {
  const wrapper = document.createElement('div');
  wrapper.className = 'list-item';

  const text = document.createElement('span');
  text.textContent = message;
  wrapper.appendChild(text);

  const tag = document.createElement('span');
  tag.className = 'tag';
  tag.textContent = tagText;
  wrapper.appendChild(tag);

  if (symbol) {
    wrapper.addEventListener('click', () => {
      setActiveSymbol(symbol, true);
    });
  }
  return wrapper;
}

function selectedChartRankCap() {
  if (!timeline) return DISPLAY_LIMIT;
  const parsed = Number(chartRankCapSelect.value);
  if (!Number.isFinite(parsed) || parsed < 1) return timeline.limit;
  return Math.min(parsed, timeline.limit);
}

function updateLatestTag() {
  if (!timeline || !timeline.dates || !timeline.dates.length) return;
  latestTag.textContent = `${timeline.dates.length}개 거래일 · 저장 상위 ${timeline.limit}위 · 차트 표시 ${selectedChartRankCap()}위`;
}

function snapshotRowsAt(index) {
  if (!timeline) return [];
  const rows = [];
  timeline.series.forEach((series) => {
    const rank = series.ranks[index];
    if (rank === null || rank === undefined) return;
    const cap = series.caps[index];
    rows.push({
      symbol: series.symbol,
      name: series.name,
      rank,
      marketCap: cap,
    });
  });
  rows.sort((a, b) => a.rank - b.rank);
  return rows;
}

function topTotalAt(index) {
  return snapshotRowsAt(index).reduce((sum, row) => sum + (row.marketCap || 0), 0);
}

function renderSnapshot() {
  if (!timeline) return;
  const index = currentDateIndex;

  const date = timeline.dates[index];
  selectedDateLabel.textContent = date;
  const rows = snapshotRowsAt(index);

  clearChildren(tableBody);
  const fragment = document.createDocumentFragment();
  rows.forEach((row) => {
    const tr = document.createElement('tr');
    if (activeSymbol && row.symbol === activeSymbol) tr.classList.add('active-row');

    const rankTd = document.createElement('td');
    const rankBadge = document.createElement('span');
    rankBadge.className = 'rank-badge';
    rankBadge.textContent = String(row.rank);
    rankTd.appendChild(rankBadge);

    const symbolTd = document.createElement('td');
    const symbolStrong = document.createElement('strong');
    symbolStrong.textContent = row.symbol;
    symbolTd.appendChild(symbolStrong);
    const nameDiv = document.createElement('div');
    nameDiv.className = 'name-subtext';
    nameDiv.textContent = row.name || '';
    symbolTd.appendChild(nameDiv);

    const capTd = document.createElement('td');
    capTd.textContent = fmtCap(row.marketCap);

    tr.appendChild(rankTd);
    tr.appendChild(symbolTd);
    tr.appendChild(capTd);

    tr.addEventListener('click', () => {
      setActiveSymbol(row.symbol, true);
    });
    fragment.appendChild(tr);
  });
  tableBody.appendChild(fragment);

  const total = topTotalAt(index);
  if (index > 0) {
    const prevTotal = topTotalAt(index - 1);
    const diff = total - prevTotal;
    const pct = prevTotal ? (diff / prevTotal) * 100 : null;
    topTotalLabel.textContent = `${fmtCap(total)} (${fmtSignedCap(diff)}, ${fmtPct(pct)})`;
  } else {
    topTotalLabel.textContent = fmtCap(total);
  }
}

function buildFlowSeries() {
  if (!timeline) return [];
  const rankCap = selectedChartRankCap();

  return timeline.series
    .filter((item) => {
      if (activeSymbol && item.symbol === activeSymbol) return true;
      const rankAtDate = item.ranks[currentDateIndex];
      return rankAtDate !== null && rankAtDate !== undefined && rankAtDate <= rankCap;
    })
    .map((item) => {
      const focused = activeSymbol && item.symbol === activeSymbol;
      return {
        type: 'line',
        name: item.symbol,
        data: item.ranks.map((v) => (v === null || v === undefined ? '-' : v)),
        connectNulls: false,
        showSymbol: false,
        smooth: 0.15,
        sampling: 'lttb',
        progressive: 2000,
        progressiveThreshold: 2000,
        lineStyle: {
          color: symbolColor(item.symbol, focused ? 1 : 0.3),
          width: focused ? 3 : 1,
        },
        emphasis: {
          focus: 'series',
        },
      };
    });
}

function renderFlowChart() {
  if (!timeline || !flowChart) return;

  const series = buildFlowSeries();
  const rankCap = selectedChartRankCap();
  const activeRank =
    activeSymbol && seriesBySymbol.has(activeSymbol)
      ? seriesBySymbol.get(activeSymbol).ranks[currentDateIndex]
      : null;
  const yMax = activeRank && activeRank > rankCap ? Math.min(timeline.limit, activeRank + 2) : rankCap;

  flowChart.setOption(
    {
      animation: false,
      backgroundColor: 'transparent',
      grid: { top: 24, left: 48, right: 16, bottom: 46 },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: timeline.dates,
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.18)' } },
        axisLabel: { color: '#9fb6d6', hideOverlap: true },
      },
      yAxis: {
        type: 'value',
        inverse: true,
        min: 1,
        max: yMax,
        interval: 5,
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.18)' } },
        axisLabel: { color: '#dfe9ff' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      dataZoom: [
        {
          type: 'inside',
          xAxisIndex: 0,
          filterMode: 'none',
        },
        {
          type: 'slider',
          xAxisIndex: 0,
          filterMode: 'none',
          height: 18,
          bottom: 8,
          borderColor: 'rgba(255,255,255,0.08)',
          backgroundColor: 'rgba(255,255,255,0.06)',
          fillerColor: 'rgba(124,245,255,0.16)',
          handleStyle: { color: '#7cf5ff' },
          textStyle: { color: '#9fb6d6' },
        },
      ],
      tooltip: {
        trigger: 'axis',
        axisPointer: { type: 'line' },
        renderMode: 'richText',
        backgroundColor: 'rgba(8,16,28,0.92)',
        borderColor: 'rgba(124,245,255,0.35)',
        textStyle: { color: '#dfe9ff' },
        formatter: (params) => {
          if (!params || !params.length) return '';
          const activePoint = activeSymbol ? params.find((p) => p.seriesName === activeSymbol) : null;
          const target = activePoint || params.find((p) => p.value !== '-') || params[0];
          const rank = target.value;
          if (rank === '-' || rank === null || rank === undefined) {
            return `${target.seriesName}: 상위 ${rankCap}위 밖`;
          }
          const seriesObj = seriesBySymbol.get(target.seriesName);
          const cap = seriesObj ? seriesObj.caps[target.dataIndex] : null;
          return `${target.axisValue}\n${target.seriesName} #${rank} · ${fmtCap(cap)}`;
        },
      },
      series,
    },
    true,
  );

  flowChart.off('click');
  flowChart.on('click', (params) => {
    if (!params || !params.seriesName) return;
    setActiveSymbol(params.seriesName, true);
  });
}

function renderDetailChart(symbol, rows) {
  if (!detailChart) return;
  const labels = rows.map((r) => r.as_of_date);
  const ranks = rows.map((r) => (r.rank === null || r.rank === undefined ? '-' : r.rank));
  const caps = rows.map((r) => {
    if (r.market_cap === null || r.market_cap === undefined) return '-';
    return r.market_cap / 1e12;
  });

  detailChart.setOption(
    {
      animation: false,
      backgroundColor: 'transparent',
      legend: {
        top: 0,
        textStyle: { color: '#dfe9ff' },
        data: ['순위', '시가총액 (조 달러)'],
      },
      grid: { top: 34, left: 52, right: 50, bottom: 32 },
      tooltip: {
        trigger: 'axis',
        renderMode: 'richText',
        backgroundColor: 'rgba(8,16,28,0.92)',
        borderColor: 'rgba(255,209,102,0.35)',
        textStyle: { color: '#dfe9ff' },
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        data: labels,
        axisLabel: { color: '#9fb6d6', hideOverlap: true },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.18)' } },
      },
      yAxis: [
        {
          type: 'value',
          inverse: true,
          min: 1,
          axisLabel: { color: '#dfe9ff' },
          axisLine: { lineStyle: { color: 'rgba(255,255,255,0.18)' } },
          splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
        },
        {
          type: 'value',
          axisLabel: {
            color: '#dfe9ff',
            formatter: (v) => `${v.toFixed(1)}조`,
          },
          axisLine: { lineStyle: { color: 'rgba(255,255,255,0.18)' } },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          type: 'line',
          name: '순위',
          data: ranks,
          showSymbol: false,
          smooth: 0.2,
          sampling: 'lttb',
          lineStyle: { color: '#7cf5ff', width: 2.2 },
        },
        {
          type: 'line',
          name: '시가총액 (조 달러)',
          yAxisIndex: 1,
          data: caps,
          showSymbol: false,
          smooth: 0.15,
          sampling: 'lttb',
          lineStyle: { color: '#ffd166', width: 1.8 },
          areaStyle: { color: 'rgba(255,209,102,0.14)' },
        },
      ],
    },
    true,
  );
  detailSelectedLabel.textContent = symbol;
}

async function setActiveSymbol(symbol, withDetail) {
  activeSymbol = symbol;
  flowSelectedLabel.textContent = symbol;
  renderFlowChart();
  renderSnapshot();
  if (withDetail) await loadHistory(symbol);
}

async function focusSymbolFromInput() {
  if (!timeline) return;
  const symbol = (symbolInput.value || '').trim().toUpperCase();
  if (!symbol) return;

  if (!seriesBySymbol.has(symbol)) {
    flowSelectedLabel.textContent = `${symbol} 데이터 없음`;
    return;
  }
  await setActiveSymbol(symbol, true);
}

async function loadTimeline() {
  historyBySymbol = new Map();
  if (timelineAbortController) {
    timelineAbortController.abort();
  }
  timelineAbortController = new AbortController();

  let res;
  try {
    res = await fetch(`/api/ranks/timeline?limit=${DISPLAY_LIMIT}&days=${LOOKBACK_DAYS}`, {
      signal: timelineAbortController.signal,
    });
  } catch (err) {
    if (err && err.name === 'AbortError') return;
    setTableMessage('타임라인 로딩 중 네트워크 오류가 발생했습니다');
    return;
  }
  if (!res.ok) {
    let message = `타임라인을 불러오지 못했습니다 (HTTP ${res.status})`;
    try {
      const data = await res.json();
      if (data && data.detail) message = String(data.detail);
    } catch (_) {
      // keep fallback
    }
    setTableMessage(message);
    return;
  }

  timeline = await res.json();
  if (!timeline.dates || !timeline.dates.length) {
    setTableMessage('타임라인 데이터가 없습니다');
    return;
  }

  seriesBySymbol = new Map(timeline.series.map((item) => [item.symbol, item]));

  const lastIndex = timeline.dates.length - 1;
  currentDateIndex = lastIndex;
  asOfLabel.textContent = timeline.dates[lastIndex];

  updateLatestTag();

  const latestRows = snapshotRowsAt(lastIndex);
  activeSymbol = latestRows.length ? latestRows[0].symbol : null;
  flowSelectedLabel.textContent = activeSymbol || '선을 클릭하거나 심볼을 입력해 종목 고정';

  renderFlowChart();
  renderSnapshot();
  if (activeSymbol) await loadHistory(activeSymbol);
}

function buildEventQuery(endpointType) {
  const params = new URLSearchParams();
  params.set('limit', String(DISPLAY_LIMIT));
  params.set('max_events', eventMaxItemsSelect.value || '260');

  const daysValue = eventDaysSelect.value;
  if (daysValue && daysValue !== 'all') {
    params.set('days', daysValue);
  }

  if (endpointType === 'movers') {
    params.set('threshold', String(BIG_MOVER_THRESHOLD));
  }
  return params.toString();
}

async function loadEntrants() {
  const res = await fetch(`/api/events/new-entrants?${buildEventQuery('entrants')}`);
  if (!res.ok) {
    clearChildren(entrantsList);
    entrantsList.appendChild(createEventItem(`신규 진입 이벤트 로딩 실패 (HTTP ${res.status})`, '오류', null));
    return;
  }
  const data = await res.json();
  const items = [...(data.events || [])].reverse();
  clearChildren(entrantsList);
  if (!items.length) {
    entrantsList.appendChild(createEventItem('아직 신규 진입 이벤트가 없습니다', '안내', null));
    return;
  }
  const fragment = document.createDocumentFragment();
  items.forEach((ev) => {
    const message = `${ev.date} · ${ev.symbol} #${ev.to_rank}로 신규 진입`;
    fragment.appendChild(createEventItem(message, '신규', ev.symbol));
  });
  entrantsList.appendChild(fragment);
}

async function loadMovers() {
  const res = await fetch(`/api/events/big-movers?${buildEventQuery('movers')}`);
  if (!res.ok) {
    clearChildren(moversList);
    moversList.appendChild(createEventItem(`급상승 이벤트 로딩 실패 (HTTP ${res.status})`, '오류', null));
    return;
  }
  const data = await res.json();
  const items = [...(data.events || [])].reverse();
  clearChildren(moversList);
  if (!items.length) {
    moversList.appendChild(createEventItem('아직 급상승 이벤트가 없습니다', '안내', null));
    return;
  }
  const fragment = document.createDocumentFragment();
  items.forEach((ev) => {
    const diff = ev.from_rank - ev.to_rank;
    const message = `${ev.date} · ${ev.symbol} ▲${diff} 상승, 현재 #${ev.to_rank}`;
    fragment.appendChild(createEventItem(message, `+${diff}`, ev.symbol));
  });
  moversList.appendChild(fragment);
}

async function loadHistory(symbol) {
  if (historyBySymbol.has(symbol)) {
    renderDetailChart(symbol, historyBySymbol.get(symbol));
    return;
  }

  if (historyAbortController) {
    historyAbortController.abort();
  }
  historyAbortController = new AbortController();

  let res;
  try {
    res = await fetch(`/api/rank-history/${encodeURIComponent(symbol)}?days=${LOOKBACK_DAYS}`, {
      signal: historyAbortController.signal,
    });
  } catch (err) {
    if (err && err.name === 'AbortError') return;
    return;
  }

  if (!res.ok) return;
  const data = await res.json();
  const rows = data.rows || [];
  historyBySymbol.set(symbol, rows);
  renderDetailChart(symbol, rows);
}

window.addEventListener('DOMContentLoaded', () => {
  flowChart = echarts.init(document.getElementById('flow-chart'), null, { renderer: 'canvas' });
  detailChart = echarts.init(document.getElementById('detail-chart'), null, { renderer: 'canvas' });

  loadTimeline();
  loadEntrants();
  loadMovers();

  chartRankCapSelect.addEventListener('change', () => {
    updateLatestTag();
    renderFlowChart();
  });

  focusSymbolButton.addEventListener('click', focusSymbolFromInput);
  symbolInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') focusSymbolFromInput();
  });

  eventDaysSelect.addEventListener('change', () => {
    loadEntrants();
    loadMovers();
  });
  eventMaxItemsSelect.addEventListener('change', () => {
    loadEntrants();
    loadMovers();
  });

  document.getElementById('refresh').addEventListener('click', () => {
    loadTimeline();
    loadEntrants();
    loadMovers();
  });

  window.addEventListener('resize', () => {
    if (flowChart) flowChart.resize();
    if (detailChart) detailChart.resize();
  });
});
