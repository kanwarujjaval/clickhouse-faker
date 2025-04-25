console.log('Starting load_drill_events.js script...');

// load_drill_events.js
import { createClient } from '@clickhouse/client';
// import { faker } from '@faker-js/faker'; // Removed faker
import { v4 as uuidv4 } from 'uuid';
import { Readable } from 'stream';
import crypto from 'crypto'; // Add crypto for hex generation

// ─────────────────────────────────────────────────────────
// 0. CONFIGURABLE PARAMETERS
// ─────────────────────────────────────────────────────────
const TOTAL_ROWS   = Number(process.env.TOTAL_ROWS) || 1_000_000;   // ≤ 4 M
const BATCH_SIZE   = 2_000;                                         // rows / insert
const SLEEP_EVERY  = 20_000;                                        // rows
const SLEEP_MS     = 1_000;                                         // 5 s

console.log(`Configuration: TOTAL_ROWS=${TOTAL_ROWS}, BATCH_SIZE=${BATCH_SIZE}, SLEEP_EVERY=${SLEEP_EVERY}, SLEEP_MS=${SLEEP_MS}`);

// ─────────────────────────────────────────────────────────
// 1. CLICKHOUSE CONNECTION
// ─────────────────────────────────────────────────────────
console.log('Establishing ClickHouse connection...');
const client = createClient({
    url: 'http://host:8123',
    username: 'default',
    password: '',
    compression: {
        request: true,
        response: false,
    },
    request_timeout: 120_000,
    application: "",
    keep_alive: {
        enabled: true,
        idle_socket_ttl: 9 * 1000,
    },
    max_open_connections: 10,
    database: 'countly_drill',
    clickhouse_settings: {
        idle_connection_timeout: 30 * 1000, // Increased timeout
        async_insert: 1,
        wait_for_async_insert: 0, // Changed to potentially speed up ingestion
        wait_end_of_query: 1,
        connect_timeout: 120,
        http_connection_timeout: 120
    },
});
console.log('ClickHouse connection established.');

// ─────────────────────────────────────────────────────────
// 2. TIMELINE (last-30-days, linear)
// ─────────────────────────────────────────────────────────
const NOW_MS         = Date.now();
const THIRTY_MS      = 30 * 24 * 60 * 60 * 1_000;
const START_MS       = NOW_MS - THIRTY_MS;
const STEP_MS        = Math.floor(THIRTY_MS / TOTAL_ROWS);          // ≈259 ms for 10 M

// choose 1 % indices to break strict ordering
const DISORDER_COUNT = Math.floor(TOTAL_ROWS * 0.01);
const DISORDER_SET = new Set();
while (DISORDER_SET.size < DISORDER_COUNT) {
  DISORDER_SET.add(Math.floor(Math.random() * TOTAL_ROWS));
}

// ─────────────────────────────────────────────────────────
// 3. STATIC DATA & HELPERS
// ─────────────────────────────────────────────────────────
const CONST_A      = crypto.randomBytes(12).toString('hex'); // Use crypto for hex
const EVENT_TYPES  = ['[CLY]_session','[CLY]_view','[CLY]_action','[CLY]_crash',
                      '[CLY]_star_rating','[CLY]_push'];
const CMP_CHANNELS = ['Organic','Direct','Email','Paid'];
const SG_KEYS      = Array.from({ length: 8000 },
                     (_,i)=>`k${(i+1).toString().padStart(4,'0')}`);
const CUSTOM_POOL  = [
  { 'Account Types':'Savings' }, { 'Account Types':'Investment' },
  { 'Communication Preference':'Phone' }, { 'Communication Preference':'Email' },
  { 'Credit Cards':'Premium' },  { 'Credit Cards':'Basic' },
  { 'Customer Type':'Retail' },  { 'Customer Type':'Business' },
  { 'Total Assets':'$0 - $50,000' },      { 'Total Assets':'$50,000 - $500,000' },
];
const LANG_CODES   = ['en', 'de', 'fr', 'es', 'pt', 'ru', 'zh', 'ja', 'ko', 'hi'];
const COUNTRY_CODES= ['US', 'DE', 'FR', 'ES', 'PT', 'RU', 'CN', 'JP', 'KR', 'IN', 'GB', 'CA', 'AU', 'BR', 'MX']; // Added static list
const PLATFORMS    = ['Macintosh','Windows','Linux','iOS','Android'];
const OS_NAMES     = ['MacOS','Windows','Android','iOS'];
const RESOLUTIONS  = ['360x640','768x1024','1920x1080'];
const BROWSERS     = ['Chrome','Firefox','Edge','Safari'];
const SOURCES      = ['MacOS','Windows','Android','iOS','Web'];
const SOURCE_CHANNELS = ['Direct','Search','Email','Social'];
const VIEW_NAMES   = ['Settings','Home','Profile', 'Dashboard', 'ProductPage', 'Checkout'];
const SAMPLE_WORDS = ['lorem', 'ipsum', 'dolor', 'sit', 'amet', 'consectetur', 'adipiscing', 'elit']; // Simple word list
const POSTFIXES    = ['S','V','A'];

// Helper function for random element
const randElement = (arr) => arr[Math.floor(Math.random() * arr.length)];
// Helper function for random integer
const randInt = (min, max) => Math.floor(Math.random() * (max - min + 1)) + min;
// Helper function for random float
const randFloat = (min, max, decimals) => Number((Math.random() * (max - min) + min).toFixed(decimals));
// Helper function for random recent timestamp (within last 7 days)
const randRecentTs = () => Math.floor((Date.now() - Math.random() * 7 * 24 * 60 * 60 * 1000) / 1000);
// Helper function for random hex string
const randHex = (bytes) => crypto.randomBytes(bytes).toString('hex');
// Helper function for random boolean
const randBool = () => Math.random() < 0.5;
// Helper function to get random sub-array
const randSubArray = (arr, min, max) => {
    const count = randInt(min, max);
    const shuffled = arr.slice().sort(() => 0.5 - Math.random()); // Simple shuffle
    return shuffled.slice(0, count);
};


// ─────────────────────────────────────────────────────────
// 3.1. UID GENERATION (7% of TOTAL_ROWS)
// ─────────────────────────────────────────────────────────
const NUM_UNIQUE_UIDS = Math.floor(TOTAL_ROWS * 0.07);
console.log(`Will generate UIDs from a pool of ${NUM_UNIQUE_UIDS} unique IDs (7% of ${TOTAL_ROWS}) on the fly.`);


function randUp () {
  const browser = randElement(BROWSERS);
  return {
    fs: randRecentTs(),
    ls: randRecentTs(),
    sc: randInt(1, 3),
    d : randElement(PLATFORMS),
    cty:'Unknown', rgn:'Unknown',
    cc : randElement(COUNTRY_CODES),
    p  : randElement(OS_NAMES),
    pv : `o${randInt(10,13)}:${randInt(0,5)}`,
    av : `${randInt(1,6)}:${randInt(0,10)}:${randInt(0,10)}`,
    c  :'Unknown',
    r  : randElement(RESOLUTIONS),
    brw: browser,
    brwv:`[${browser}]_${randInt(100,140)}:0:0:0`,
    la : randElement(LANG_CODES),
    src: randElement(SOURCES),
    src_ch: randElement(SOURCE_CHANNELS),
    lv : randElement(VIEW_NAMES),
    hour: randInt(0, 23),
    dow : randInt(0, 6),
  };
}

function makeRow (idx) {
  let ts = START_MS + idx * STEP_MS;
  if (DISORDER_SET.has(idx)) {
    ts += randInt(-2 * STEP_MS, 2 * STEP_MS);
    ts = Math.max(START_MS, Math.min(ts, NOW_MS));
  }

  // Deterministically generate UID based on index modulo the desired number of unique UIDs
  const uidBucketIndex = idx % NUM_UNIQUE_UIDS;
  const selectedUid = uidBucketIndex; // Use the numeric bucket index directly as the UID
  const _id  = `${randHex(20)}_${selectedUid}_${ts}`; // Use selectedUid for _id
  const sgSel= randSubArray(SG_KEYS, 15, 20);
  const sgObj= Object.fromEntries(sgSel.map(k=>[k, randElement(SAMPLE_WORDS)]));
  Object.assign(sgObj,{
    request_id:_id,
    postfix: randElement(POSTFIXES),
    ended: randBool().toString()
  });

  return {
    a: CONST_A,
    e: randElement(EVENT_TYPES),
    uid: selectedUid, // Use the deterministically generated UID
    did: uuidv4(),
    lsid: _id,
    _id,
    ts,
    up: randUp(),
    custom: randElement(CUSTOM_POOL),
    cmp: { c: randElement(CMP_CHANNELS) },
    sg: sgObj,
    c: randInt(1, 5),
    s: randFloat(0, 1, 6),
    dur: randInt(100, 90000)
  };
}

// streaming generator - reads from pre-generated array
function batchStream (batchData) {
  let index = 0;
  return new Readable({
    objectMode:true,
    read() {
      if (index >= batchData.length) {
        this.push(null); // Signal end of stream
      } else {
        this.push(batchData[index]);
        index++;
      }
    }
  });
}

// promisified sleep
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─────────────────────────────────────────────────────────
// 4. MAIN INGESTION LOOP
// ─────────────────────────────────────────────────────────
(async () => {
  console.log('Starting main ingestion loop...');
  console.time(`insert-${TOTAL_ROWS/1e6}M`);

  const BATCHES = Math.ceil(TOTAL_ROWS / BATCH_SIZE);
  console.log(`Total batches to process: ${BATCHES}`);

  for (let b = 0; b < BATCHES; b++) {
    const globalStart = b * BATCH_SIZE;
    console.log(`Generating data for batch ${b + 1}/${BATCHES}, starting at row ${globalStart}...`);
    console.time(`batch-generate-${b + 1}`);
    const batchData = Array.from({ length: BATCH_SIZE }, (_, i) => makeRow(globalStart + i));
    console.timeEnd(`batch-generate-${b + 1}`);

    console.log(`Starting write for batch ${b + 1}/${BATCHES}...`);
    console.time(`batch-write-${b + 1}`);
    const stream = batchStream(batchData); // Pass pre-generated data
    await client.insert({
      table: 'drill_events',
      values: stream,
      format:'JSONEachRow',
    });
    console.timeEnd(`batch-write-${b + 1}`);
    console.log(`Finished write for batch ${b + 1}/${BATCHES}.`);

    const doneRows = (b + 1) * BATCH_SIZE;
    console.log(`✔ inserted ${doneRows.toLocaleString()} / ${TOTAL_ROWS.toLocaleString()}`);

    // throttle every 250 000 rows
    if (doneRows % SLEEP_EVERY === 0 && doneRows < TOTAL_ROWS) {
      console.log(`⏳ sleeping ${SLEEP_MS/1000}s to throttle ingestion…`);
      await sleep(SLEEP_MS);
    }
  }

  console.timeEnd(`insert-${TOTAL_ROWS/1e6}M`);
  console.log('Main ingestion loop finished.');
  await client.close();
  console.log('ClickHouse connection closed.');
})().catch(err=>{
  console.error('An error occurred during ingestion:', err);
  process.exit(1);
});
