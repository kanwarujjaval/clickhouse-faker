console.log('Starting load_drill_events.js script...');

// load_drill_events.js
import { createClient } from '@clickhouse/client';
import { faker } from '@faker-js/faker';
import { v4 as uuidv4 } from 'uuid';
import { Readable } from 'stream';

// ─────────────────────────────────────────────────────────
// 0. CONFIGURABLE PARAMETERS
// ─────────────────────────────────────────────────────────
const TOTAL_ROWS   = Number(process.env.TOTAL_ROWS) || 5_000_000;   // ≤ 4 M
const BATCH_SIZE   = 25_000;                                         // rows / insert
const SLEEP_EVERY  = 250_000;                                        // rows
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
const DISORDER_SET = new Set(
  faker.helpers.uniqueArray(
    () => faker.number.int({ min: 0, max: TOTAL_ROWS - 1 }),
    Math.floor(TOTAL_ROWS * 0.01)
  )
);

// ─────────────────────────────────────────────────────────
// 3. STATIC DATA & HELPERS
// ─────────────────────────────────────────────────────────
const CONST_A      = faker.string.hexadecimal({ length: 24, prefix: '' });
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

// ─────────────────────────────────────────────────────────
// 3.1. UID GENERATION (7% of TOTAL_ROWS)
// ─────────────────────────────────────────────────────────
const NUM_UNIQUE_UIDS = Math.floor(TOTAL_ROWS * 0.07);
console.log(`Will generate UIDs from a pool of ${NUM_UNIQUE_UIDS} unique IDs (7% of ${TOTAL_ROWS}) on the fly.`);


function randUp () {
  return {
    fs: faker.date.recent().getTime()/1000|0,
    ls: faker.date.recent().getTime()/1000|0,
    sc: faker.number.int({ min:1,max:3 }),
    d : faker.helpers.arrayElement(['Macintosh','Windows','Linux','iOS','Android']),
    cty:'Unknown', rgn:'Unknown',
    cc : faker.location.countryCode(),
    p  : faker.helpers.arrayElement(['MacOS','Windows','Android','iOS']),
    pv : `o${faker.number.int({min:10,max:13})}:${faker.number.int({min:0,max:5})}`,
    av : `${faker.number.int({min:1,max:6})}:${faker.number.int({min:0,max:10})}:${faker.number.int({min:0,max:10})}`,
    c  :'Unknown',
    r  : faker.helpers.arrayElement(['360x640','768x1024','1920x1080']),
    brw: faker.helpers.arrayElement(['Chrome','Firefox','Edge','Safari']),
    brwv:`[${faker.helpers.arrayElement(['Chrome','Firefox','Edge','Safari'])}]_${faker.number.int({min:100,max:140})}:0:0:0`,
    la : faker.helpers.arrayElement(LANG_CODES),
    src: faker.helpers.arrayElement(['MacOS','Windows','Android','iOS','Web']),
    src_ch: faker.helpers.arrayElement(['Direct','Search','Email','Social']),
    lv : faker.helpers.arrayElement(['Settings','Home','Profile']),
    hour: faker.number.int({ min:0,max:23 }),
    dow : faker.number.int({ min:0,max:6 }),
  };
}

function makeRow (idx) {
  let ts = START_MS + idx * STEP_MS;
  if (DISORDER_SET.has(idx)) {
    ts += faker.number.int({ min:-2*STEP_MS, max:2*STEP_MS });
    ts = Math.max(START_MS, Math.min(ts, NOW_MS));
  }

  // Deterministically generate UID based on index modulo the desired number of unique UIDs
  const uidBucketIndex = idx % NUM_UNIQUE_UIDS;
  const selectedUid = uidBucketIndex; // Use the numeric bucket index directly as the UID
  const _id  = `${faker.string.hexadecimal({length:40}).slice(2)}_${selectedUid}_${ts}`; // Use selectedUid for _id
  const sgSel= faker.helpers.arrayElements(SG_KEYS,{min:15,max:20});
  const sgObj= Object.fromEntries(sgSel.map(k=>[k,faker.word.sample()]));
  Object.assign(sgObj,{
    request_id:_id,
    postfix:faker.helpers.arrayElement(['S','V','A']),
    ended:faker.datatype.boolean().toString()
  });

  return {
    a: CONST_A,
    e: faker.helpers.arrayElement(EVENT_TYPES),
    uid: selectedUid, // Use the deterministically generated UID
    did: uuidv4(),
    lsid: _id,
    _id,
    ts,
    up: randUp(),
    custom: faker.helpers.arrayElement(CUSTOM_POOL),
    cmp: { c: faker.helpers.arrayElement(CMP_CHANNELS) },
    sg: sgObj,
    c: faker.number.int({min:1,max:5}),
    s: Number(faker.finance.amount({min:0,max:1,dec:6})),
    dur: faker.number.int({min:100,max:90000})
  };
}

// streaming generator – 25 000 rows
function batchStream (startIdx, size) {
  let sent = 0;
  return new Readable({
    objectMode:true,
    read() {
      if (sent >= size) return this.push(null);
      this.push(makeRow(startIdx + sent));
      sent++;
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
    console.log(`Processing batch ${b + 1}/${BATCHES}, starting at row ${globalStart}`);
    const stream      = batchStream(globalStart, BATCH_SIZE);

    await client.insert({
      table: 'drill_events',
      values: stream,
      format:'JSONEachRow',
    });

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
