const DISPLAY_LIMIT = 260;
const LOOKBACK_DAYS = 5475;
const BIG_MOVER_THRESHOLD = 5;
const FLOW_MAX_POINTS_MIN = 480;
const FLOW_MAX_POINTS_MAX = 1400;
const FLOW_POINTS_PER_PX = 1.15;

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
const flowChartElement = document.getElementById('flow-chart');

let flowChart = null;
let detailChart = null;
let timeline = null;
let activeSymbol = null;
let currentDateIndex = 0;
let seriesBySymbol = new Map();
let historyBySymbol = new Map();
let historyAbortController = null;
let timelineAbortController = null;
let latestSnapshotRows = [];
let latestSnapshotBySymbol = new Map();
let latestTotalMarketCap = null;
let flowBaseSignature = '';
let symbolHueBySymbol = new Map();

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

function symbolHue(symbol) {
  if (symbolHueBySymbol.has(symbol)) return symbolHueBySymbol.get(symbol);
  let hash = 0;
  for (let i = 0; i < symbol.length; i += 1) {
    hash = (hash * 31 + symbol.charCodeAt(i)) % 360;
  }
  symbolHueBySymbol.set(symbol, hash);
  return hash;
}

function symbolColor(symbol, alpha = 1) {
  return `hsla(${symbolHue(symbol)}, 80%, 60%, ${alpha})`;
}

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

function suggestedFlowMaxPoints() {
  const width = Math.max(
    window.innerWidth || 0,
    (flowChartElement && flowChartElement.clientWidth) || 0,
    360,
  );
  const estimated = Math.round(width * FLOW_POINTS_PER_PX);
  return Math.max(FLOW_MAX_POINTS_MIN, Math.min(FLOW_MAX_POINTS_MAX, estimated));
}

function updateLatestTag() {
  if (!timeline || !timeline.dates || !timeline.dates.length) return;
  const totalDates = Number(timeline.total_dates) > 0 ? timeline.total_dates : timeline.dates.length;
  const renderedDates = timeline.dates.length;
  const sampledLabel =
    renderedDates < totalDates
      ? `${renderedDates}개 샘플 (약 ${timeline.sampling_step || 1}일 간격)`
      : `${renderedDates}개 거래일`;
  latestTag.textContent = `${totalDates}개 거래일(${sampledLabel}) · 저장 상위 ${timeline.limit}위 · 차트 표시 ${selectedChartRankCap()}위`;
}

function setLatestSnapshot(data) {
  const rows = Array.isArray(data && data.rows) ? data.rows : [];
  latestSnapshotRows = rows
    .map((row) => ({
      symbol: row.symbol,
      name: row.name || row.symbol,
      rank: row.rank,
      marketCap: row.market_cap,
    }))
    .sort((a, b) => a.rank - b.rank);
  latestSnapshotBySymbol = new Map(latestSnapshotRows.map((row) => [row.symbol, row]));
  latestTotalMarketCap = latestSnapshotRows.reduce(
    (sum, row) => sum + (Number.isFinite(row.marketCap) ? row.marketCap : 0),
    0,
  );
}

function snapshotRowsAt(index) {
  if (!timeline) return [];

  const lastIndex = timeline.dates.length - 1;
  if (index === lastIndex && latestSnapshotRows.length) {
    return latestSnapshotRows;
  }

  const includeCaps = Boolean(timeline.include_caps);
  const rows = [];
  timeline.series.forEach((series) => {
    const rank = series.ranks[index];
    if (rank === null || rank === undefined) return;

    const cap = includeCaps && Array.isArray(series.caps) ? series.caps[index] : null;
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
  if (!timeline) return null;

  const lastIndex = timeline.dates.length - 1;
  if (index === lastIndex && Number.isFinite(latestTotalMarketCap)) {
    return latestTotalMarketCap;
  }

  const rows = snapshotRowsAt(index);
  let hasCap = false;
  let sum = 0;
  rows.forEach((row) => {
    if (Number.isFinite(row.marketCap)) {
      hasCap = true;
      sum += row.marketCap;
    }
  });
  return hasCap ? sum : null;
}

function renderSnapshot() {
  if (!timeline) return;
  const index = currentDateIndex;
  const date = timeline.dates[index];
  selectedDateLabel.textContent = date;

  const rows = snapshotRowsAt(index);
  if (!rows.length) {
    setTableMessage('표시할 순위 데이터가 없습니다');
    topTotalLabel.textContent = '—';
    return;
  }

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
    if (Number.isFinite(total) && Number.isFinite(prevTotal) && prevTotal > 0) {
      const diff = total - prevTotal;
      const pct = (diff / prevTotal) * 100;
      topTotalLabel.textContent = `${fmtCap(total)} (${fmtSignedCap(diff)}, ${fmtPct(pct)})`;
      return;
    }
  }
  topTotalLabel.textContent = fmtCap(total);
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
        data: item.plotRanks,
        connectNulls: false,
        showSymbol: false,
        smooth: false,
        sampling: 'lttb',
        progressive: 4000,
        progressiveThreshold: 3000,
        large: true,
        largeThreshold: 1200,
        animation: false,
        lineStyle: {
          color: symbolColor(item.symbol, focused ? 1 : 0.24),
          width: focused ? 2.6 : 1,
        },
        emphasis: {
          focus: 'series',
        },
      };
    });
}

function flowChartSignature() {
  if (!timeline || !timeline.dates || !timeline.dates.length) return 'empty';
  const first = timeline.dates[0];
  const last = timeline.dates[timeline.dates.length - 1];
  return `${timeline.dates.length}:${first}:${last}`;
}

function formatFlowTooltip(param) {
  if (!param) return '';

  const rank = param.value;
  if (rank === '-' || rank === null || rank === undefined) {
    return `${param.seriesName}: 데이터 없음`;
  }

  const idx = Number.isInteger(param.dataIndex) ? param.dataIndex : -1;
  const label = idx >= 0 && timeline && timeline.dates ? timeline.dates[idx] : '';
  const seriesObj = seriesBySymbol.get(param.seriesName);

  let cap = null;
  if (seriesObj && Array.isArray(seriesObj.caps) && idx >= 0 && idx < seriesObj.caps.length) {
    cap = seriesObj.caps[idx];
  }
  if ((cap === null || cap === undefined) && timeline && idx === timeline.dates.length - 1) {
    const latestRow = latestSnapshotBySymbol.get(param.seriesName);
    cap = latestRow ? latestRow.marketCap : null;
  }

  return `${label}\n${param.seriesName} #${rank} · ${fmtCap(cap)}`;
}

function renderFlowChart() {
  if (!timeline || !flowChart) return;

  const signature = flowChartSignature();
  if (flowBaseSignature !== signature) {
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
          max: selectedChartRankCap(),
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
          trigger: 'item',
          renderMode: 'richText',
          confine: true,
          backgroundColor: 'rgba(8,16,28,0.92)',
          borderColor: 'rgba(124,245,255,0.35)',
          textStyle: { color: '#dfe9ff' },
          formatter: formatFlowTooltip,
        },
        series: [],
      },
      {
        notMerge: true,
        lazyUpdate: true,
      },
    );
    flowBaseSignature = signature;
  }

  const series = buildFlowSeries();
  const rankCap = selectedChartRankCap();
  const activeRank =
    activeSymbol && seriesBySymbol.has(activeSymbol)
      ? seriesBySymbol.get(activeSymbol).ranks[currentDateIndex]
      : null;
  const yMax = activeRank && activeRank > rankCap ? Math.min(timeline.limit, activeRank + 2) : rankCap;

  flowChart.setOption(
    {
      yAxis: {
        max: yMax,
      },
      series,
    },
    {
      notMerge: false,
      lazyUpdate: true,
      replaceMerge: ['series'],
      silent: true,
    },
  );
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
          smooth: false,
          sampling: 'lttb',
          lineStyle: { color: '#7cf5ff', width: 2.2 },
        },
        {
          type: 'line',
          name: '시가총액 (조 달러)',
          yAxisIndex: 1,
          data: caps,
          showSymbol: false,
          smooth: false,
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

  const flowMaxPoints = suggestedFlowMaxPoints();
  const query = new URLSearchParams();
  query.set('limit', String(DISPLAY_LIMIT));
  query.set('days', String(LOOKBACK_DAYS));
  query.set('max_points', String(flowMaxPoints));
  query.set('include_caps', '0');

  let timelineRes;
  let latestRes;
  try {
    [timelineRes, latestRes] = await Promise.all([
      fetch(`/api/ranks/timeline?${query.toString()}`, {
        signal: timelineAbortController.signal,
      }),
      fetch(`/api/ranks/latest?limit=${DISPLAY_LIMIT}`),
    ]);
  } catch (err) {
    if (err && err.name === 'AbortError') return;
    setTableMessage('타임라인 로딩 중 네트워크 오류가 발생했습니다');
    return;
  }

  if (!timelineRes.ok) {
    let message = `타임라인을 불러오지 못했습니다 (HTTP ${timelineRes.status})`;
    try {
      const data = await timelineRes.json();
      if (data && data.detail) message = String(data.detail);
    } catch (_) {
      // keep fallback
    }
    setTableMessage(message);
    return;
  }

  timeline = await timelineRes.json();
  if (!timeline.dates || !timeline.dates.length) {
    setTableMessage('타임라인 데이터가 없습니다');
    return;
  }

  timeline.series.forEach((item) => {
    item.plotRanks = item.ranks.map((value) => (value === null || value === undefined ? '-' : value));
  });
  seriesBySymbol = new Map(timeline.series.map((item) => [item.symbol, item]));

  let latestAsOfDate = null;
  if (latestRes.ok) {
    const latestData = await latestRes.json();
    setLatestSnapshot(latestData);
    latestAsOfDate = latestData && latestData.as_of_date ? latestData.as_of_date : null;
  } else {
    latestSnapshotRows = [];
    latestSnapshotBySymbol = new Map();
    latestTotalMarketCap = null;
  }

  const lastIndex = timeline.dates.length - 1;
  currentDateIndex = lastIndex;
  asOfLabel.textContent = latestAsOfDate || timeline.dates[lastIndex];

  updateLatestTag();

  const latestRows = snapshotRowsAt(lastIndex);
  activeSymbol = latestRows.length ? latestRows[0].symbol : null;
  flowSelectedLabel.textContent = activeSymbol || '선을 클릭하거나 심볼을 입력해 종목 고정';

  flowBaseSignature = '';
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
  flowChart = echarts.init(flowChartElement, null, { renderer: 'canvas' });
  detailChart = echarts.init(document.getElementById('detail-chart'), null, { renderer: 'canvas' });

  flowChart.on('click', (params) => {
    if (!params || !params.seriesName) return;
    setActiveSymbol(params.seriesName, true);
  });

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
