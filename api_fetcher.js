// api_fetcher.js — maker × month registrations from the analytics.parivahan.gov.in JSON API
// node api_fetcher.js                     — YEAR env (default 2026), all states
// STATES=DL,CH CONCURRENCY=5 node api_fetcher.js
// YEAR=2024 REFETCH=1 node api_fetcher.js — refill a past year (upserts only, never deletes)
// node api_fetcher.js --check             — offline self-check of the parsing logic
//
// The report API needs a session unlocked by a captcha (~15 min, not extended by use).
// Captchas are solved with tesseract.js on CPU; the session is re-unlocked every 13 min
// or as soon as the API says it expired.

require('dotenv').config();
const assert = require('assert');
const { createWorker } = require('tesseract.js');
const db = require('./db');
const { parseSelects } = require('./fetch_dropdowns');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const BASE        = 'https://analytics.parivahan.gov.in/analytics';
const PAGE_URL    = `${BASE}/vahanpublicreport?lang=en`;
const UA          = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36';
const FETCH_YEAR  = parseInt(process.env.YEAR || new Date().getFullYear()); // daily run must follow the calendar
const STATES      = process.env.STATES ? process.env.STATES.split(',') : null;
// load test: 5 concurrent ≈ 34 req/s with flat latency; 10 only reached 42 req/s with latency climbing
const CONCURRENCY = parseInt(process.env.CONCURRENCY || '5');
const UNLOCK_MS   = 13 * 60 * 1000;
const MAX_RETRIES = 8;
const MONTHS      = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

// Calendar year, Maker × Month Wise, no other filters (filters get appended per combo)
const FORM = 'archivedFlags=ACTIVE_COMPLIANT&archivedFlags=ACTIVE_NON_COMPLIANT&archivedFlags=PERMANENT_ARCHIVE&archivedFlags=TEMPORARY_ARCHIVE&_archivedFlags=1'
    + `&timePeriod=0&_financialYearList=1&fromYear=${FETCH_YEAR}&toYear=${FETCH_YEAR}&fromDate=1+Jan+${FETCH_YEAR}&toDate=31+Dec+${FETCH_YEAR}&reportYear=${FETCH_YEAR}&reportMonth=`
    + '&_stateMultiple=1&_rtoCodeMultiple=1&_vehicleEmissions=1&_vehicleMakers=1&selectedMakersCsv=&_vehicleCategoryGroup=1&_vehicleSubCategories=1'
    + '&_vehicleClasses=1&_vehicleFuels=1&_evType=1&_vehicleStatus=1&_vehicleOwnerType=1&vehicleType=&fitnessCheck=0&delhiNcr=0'
    + '&yAxis=vehicleMakerName&xAxis=monthWise&last5financialYearList=';
// ─────────────────────────────────────────────────────────────────────────────

const sleep = ms => new Promise(r => setTimeout(r, ms));
const log   = (...a) => console.log(new Date().toTimeString().slice(0, 8), ...a);
const warn  = (...a) => console.warn(new Date().toTimeString().slice(0, 8), ...a);

// ─── PURE HELPERS ────────────────────────────────────────────────────────────

// { MAKER: { "2026-Jan": 13, ... } } → [{ maker, month, count }] (non-zero, requested year only)
function toRecords(rows, year) {
    const records = [];
    for (const [maker, months] of Object.entries(rows || {})) {
        for (const [key, value] of Object.entries(months)) {
            const [y, mon] = key.split('-');
            const month = MONTHS.indexOf(mon) + 1;
            // numbers today, but "1,343"-style strings once truncated a whole year of data
            const count = typeof value === 'number' ? value : parseInt(String(value).replace(/,/g, ''));
            if (+y === year && month > 0 && count > 0) records.push({ maker: maker.trim(), month, count });
        }
    }
    return records;
}

// Site rtoCode for a DB rto code. The production DB stores it as-is ("4", as app.js wrote it);
// an older local copy has "DL4" (Odisha "OD4"). Anything else, e.g. "AS28" filed under GJ, → null.
const rtoNumber = (stateCode, code) =>
    String(code).match(new RegExp(`^(?:${stateCode === 'OR' ? 'OD' : stateCode})?(\\d+)$`))?.[1] ?? null;

// Site labels differ from DB labels in case/spacing/symbols: "Tractor-Trolley(Commercial)" vs "TRACTOR-TROLLEY (COMMERCIAL)"
const classKey = label => label.toUpperCase().replace(/[^A-Z0-9]/g, '');

// ─── HTTP SESSION ────────────────────────────────────────────────────────────

async function request(session, url, body) {
    const res = await fetch(url, {
        method: body ? 'POST' : 'GET',
        body: body && `${FORM}&_csrf=${session.csrf}&${body}`,
        redirect: 'manual',
        signal: AbortSignal.timeout(60_000),
        headers: {
            'User-Agent': UA,
            Referer: PAGE_URL,
            Origin: 'https://analytics.parivahan.gov.in',
            Cookie: [...session.cookies].map(([k, v]) => `${k}=${v}`).join('; '),
            ...(body && { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' }),
        },
    });
    for (const c of res.headers.getSetCookie()) {
        const [kv] = c.split(';');
        const i = kv.indexOf('=');
        session.cookies.set(kv.slice(0, i), kv.slice(i + 1));
    }
    return res;
}

let ocr;
let session   = null;
let unlocking = null;

async function unlockNewSession() {
    const s = { cookies: new Map() };
    const page = await request(s, PAGE_URL);
    s.html = await page.text();
    s.csrf = s.html.match(/name="_csrf" value="([^"]+)"/)?.[1];
    if (!s.csrf) throw new Error(`No _csrf on page (HTTP ${page.status})`);

    if (!ocr) {
        ocr = await createWorker('eng', 1, { cachePath: __dirname });
        await ocr.setParameters({
            tessedit_char_whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
            tessedit_pageseg_mode: '7', // single line
        });
    }

    for (let attempt = 1; attempt <= 5; attempt++) {
        const img   = Buffer.from(await (await request(s, `${BASE}/captcha-gen?_ts=${Date.now()}&_seq=${attempt}`)).arrayBuffer());
        const guess = (await ocr.recognize(img)).data.text.replace(/\s/g, '');
        await (await request(s, PAGE_URL, `captcha=${encodeURIComponent(guess)}`)).text();

        // Chandigarh / one class: cheapest query that proves the unlock
        const probe = await request(s, `${BASE}/vahanpublicreport/maker-report-page`, 'stateMultiple=CH&vehicleClasses=Motor+Car&pageSize=25&afterMaker=');
        await probe.text();
        if (probe.status === 200) {
            s.unlockedAt = Date.now();
            log(`[CAPTCHA] unlocked (attempt ${attempt}, "${guess}")`);
            return s;
        }
    }
    throw new Error('Captcha not solved in 5 attempts');
}

// Shared by all concurrent tasks: only one captcha solve runs at a time
async function getSession() {
    if (session && Date.now() - session.unlockedAt < UNLOCK_MS) return session;
    unlocking ??= unlockNewSession()
        .then(s => (session = s))
        .finally(() => (unlocking = null));
    return unlocking;
}

async function withRetry(fn, label) {
    for (let attempt = 1; ; attempt++) {
        try {
            return await fn();
        } catch (err) {
            if (attempt >= MAX_RETRIES) throw err;
            const delay = err.sessionExpired ? 0 : Math.min(5_000 * 2 ** (attempt - 1), 300_000);
            warn(`  [RETRY ${attempt}/${MAX_RETRIES}] ${label}: ${err.message}${delay ? ` — waiting ${delay / 1000}s` : ''}`);
            await sleep(delay);
        }
    }
}

async function getJson(url, label) {
    return withRetry(async () => {
        const res = await fetch(url, { headers: { 'User-Agent': UA, Referer: PAGE_URL }, signal: AbortSignal.timeout(60_000) });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
    }, label);
}

// All pages of one combo (the server caps pages at 25 makers)
async function fetchReport(filters, label) {
    const rows = {};
    let after = '';
    do {
        const data = await withRetry(async () => {
            const s = await getSession();
            const res = await request(s, `${BASE}/vahanpublicreport/maker-report-page`,
                `${filters}&pageSize=25&afterMaker=${encodeURIComponent(after)}`);
            const body = await res.text();
            if (res.status === 401 || (res.status === 403 && /captcha/i.test(body))) {
                if (session === s) s.unlockedAt = 0;
                throw Object.assign(new Error('session expired'), { sessionExpired: true });
            }
            if (res.status !== 200) throw new Error(`HTTP ${res.status} ${body.slice(0, 100)}`);
            return JSON.parse(body);
        }, label);
        Object.assign(rows, data.rows);
        after = data.hasMore ? data.nextAfterMaker : '';
    } while (after);
    return rows;
}

async function runPool(items, size, fn) {
    let next = 0;
    await Promise.all(Array.from({ length: size }, async () => {
        while (next < items.length) await fn(items[next++]);
    }));
}

// ─── MAIN ────────────────────────────────────────────────────────────────────

async function main() {
    await db.initDb();
    const [allStates, vehicleClasses, done, dbRtos] = await Promise.all([
        db.loadStates(),
        db.loadVehicleClasses(),
        // REFETCH=1: redo combos already marked done — past years are upsert-only, so this fills gaps without deleting rows
        process.env.REFETCH ? new Set() : db.loadCompleted(FETCH_YEAR),
        db.loadAllRtos(),
    ]);
    const states = STATES ? allStates.filter(s => STATES.includes(s.code)) : allStates;
    log(`Year ${FETCH_YEAR}, ${states.length} states, concurrency ${CONCURRENCY}, ${done.size} combos already done`);

    // Site class values → DB vehicle_classes rows
    const vcByKey = new Map(vehicleClasses.map(vc => [classKey(vc.label), vc]));
    const classes = [];
    for (const { value } of parseSelects((await getSession()).html).vehicleClass) {
        const vc = vcByKey.get(classKey(value));
        if (vc) classes.push({ value, vc });
        else warn(`[WARN] site class "${value}" has no vehicle_classes row — skipped`);
    }
    log(`${classes.length} vehicle classes mapped`);

    for (const state of states) {
        const t0 = Date.now();
        // Reuse the existing rtos row for each RTO number, whatever its code format; never rename it
        const dbByNumber = new Map();
        for (const r of dbRtos.get(state.code) || []) {
            const n = rtoNumber(state.code, r.value);
            if (n && !dbByNumber.has(n)) dbByNumber.set(n, r);
        }
        const siteRtos = (await getJson(`${BASE}/json_rtos?stateCode=${state.code}`, `RTOs ${state.code}`))
            .map(r => ({ value: dbByNumber.get(String(r.rtoCode))?.value ?? String(r.rtoCode), siteCode: String(r.rtoCode), text: r.rtoName }));
        await db.saveRtos(state.code, siteRtos.filter(r => !dbByNumber.has(r.siteCode))); // new RTOs only

        // The site's RTO dropdown omits some RTOs the report API still has data for
        // (AP837, AS36, MZ10/11: 2025 state totals only add up with them), so also fetch every RTO already in the DB
        const extraRtos = [...dbByNumber]
            .filter(([n]) => !siteRtos.some(s => s.siteCode === n))
            .map(([n, r]) => ({ ...r, siteCode: n }));
        const rtos = [...siteRtos, ...extraRtos];

        const combos = rtos.flatMap(rto => classes.map(c => ({ rto, c })))
            .filter(({ rto, c }) => !done.has(`${state.code}|${rto.value}|${c.vc.idx}|${FETCH_YEAR}`));
        log(`STATE ${state.name} (${state.code}): ${siteRtos.length} site RTOs + ${extraRtos.length} DB-only, ${combos.length} combos to fetch`);

        let rowCount = 0, errors = 0;
        await runPool(combos, CONCURRENCY, async ({ rto, c }) => {
            const label = `${state.code}/${rto.value}/${c.vc.label}`;
            try {
                const rows = await fetchReport(
                    `stateMultiple=${state.code}&rtoCodeMultiple=${rto.siteCode}&vehicleClasses=${encodeURIComponent(c.value)}`, label);
                const records = toRecords(rows, FETCH_YEAR);
                await db.saveRecords(state.code, rto.value, c.vc.idx, FETCH_YEAR, records);
                await db.markCompleted(state.code, rto.value, c.vc.idx, FETCH_YEAR);
                await db.logFetch(state.code, rto.value, c.vc.idx, 'success', `${records.length} rows (api)`);
                rowCount += records.length;
            } catch (err) {
                errors++;
                warn(`  [FAILED] ${label} — ${err.message}`);
                await db.logFetch(state.code, rto.value, c.vc.idx, 'error', err.message);
            }
        });
        log(`  ${state.code} done: ${rowCount} rows, ${errors} errors, ${((Date.now() - t0) / 1000).toFixed(0)}s`);
    }

    await ocr?.terminate();
    await db.closeDb();
    log('All done ✓');
}

function selfCheck() {
    assert.deepStrictEqual(
        toRecords({ ' HONDA ': { '2026-Jan': 5, '2026-Feb': 0, '2026-Mar': '1,343', '2025-Dec': 9, '2026-Foo': 3 } }, 2026),
        [{ maker: 'HONDA', month: 1, count: 5 }, { maker: 'HONDA', month: 3, count: 1343 }]);
    assert.strictEqual(classKey('Tractor-Trolley(Commercial)'), classKey('TRACTOR-TROLLEY (COMMERCIAL)'));
    assert.strictEqual(classKey('Motorised Cycle (CC  25cc)'), classKey('MOTORISED CYCLE (CC > 25CC)'));
    assert.strictEqual(rtoNumber('DL', '4'), '4');
    assert.strictEqual(rtoNumber('DL', 'DL4'), '4');
    assert.strictEqual(rtoNumber('OR', 'OD2'), '2');
    assert.strictEqual(rtoNumber('GJ', 'AS28'), null);
    console.log('self-check ok');
}

if (require.main === module) {
    if (process.argv.includes('--check')) selfCheck();
    else main().catch(async err => {
        console.error('Fatal:', err);
        await db.closeDb().catch(() => {});
        process.exit(1);
    });
}

module.exports = { BASE, getJson, fetchReport, toRecords, classKey, rtoNumber };
