// app.js — VAHAN Dashboard Scraper (v6 - Postgres-backed progress & logs)
// node app.js

require('dotenv').config();
const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const db = require('./db');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const BASE_URL = 'https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml';
const DOWNLOAD_DIR = path.resolve('./vahan_downloads');
const KEEPALIVE_MS = 3.5 * 60 * 1000;
const RETRY_DELAY_MS = 20_000;
const MAX_RETRIES = 6;
const AJAX_TIMEOUT_MS = 90_000;
const NAV_TIMEOUT_MS = 90_000;
const STEP_DELAY_MS = 2_000;
// ─────────────────────────────────────────────────────────────────────────────

function comboKey(stateCode, rtoCode, vcIdx) {
    return `${stateCode}|${rtoCode}|${vcIdx}`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function withRetry(fn, label, maxRetries = MAX_RETRIES) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await fn();
        } catch (err) {
            const msg = String(err.message || err);
            const isServerErr = /50[023]|gateway|timeout|ECONNRESET|net::|server error/i.test(msg);
            console.warn(`  [RETRY ${attempt}/${maxRetries}] ${label}`);
            console.warn(`    Reason: ${msg.split('\n')[0]}`);
            if (attempt === maxRetries) throw err;
            const delay = isServerErr ? RETRY_DELAY_MS * 2 : RETRY_DELAY_MS;
            console.log(`    Waiting ${delay / 1000}s…`);
            await sleep(delay);
        }
    }
}

// ─── AJAX WAIT ────────────────────────────────────────────────────────────────
async function waitForAjax(page, timeoutMs = AJAX_TIMEOUT_MS) {
    await sleep(300);
    await page.waitForFunction(() => {
        const blocker = document.querySelector('.ui-blockui');
        if (!blocker) return true;
        return blocker.style.display !== 'block';
    }, { timeout: timeoutMs });
    await sleep(STEP_DELAY_MS);
}

// ─── PRIMEFACES SELECT ────────────────────────────────────────────────────────
async function pfSelect(page, widgetKey, value) {
    await page.evaluate(({ widgetKey, value }) => {
        let w = PrimeFaces.widgets[widgetKey];
        if (!w) {
            for (const k of Object.keys(PrimeFaces.widgets)) {
                const ww = PrimeFaces.widgets[k];
                if (ww && ww.input && ww.input[0] && ww.input[0].name === widgetKey) {
                    w = ww; break;
                }
            }
        }
        if (!w) throw new Error(`PrimeFaces widget not found: ${widgetKey}`);
        w.selectValue(value);
    }, { widgetKey, value });
    await waitForAjax(page);
}

async function getStateWidgetKey(page) {
    return page.evaluate(() => {
        for (const k of Object.keys(PrimeFaces.widgets)) {
            const w = PrimeFaces.widgets[k];
            if (w && w.input && w.input[0]) {
                const oc = w.input[0].getAttribute('onchange') || '';
                if (oc.includes('selectedRto yaxisVar')) return k;
            }
        }
        return null;
    });
}

// ─── CLICK MAIN REFRESH ───────────────────────────────────────────────────────
async function clickMainRefresh(page) {
    await page.evaluate(() => {
        const filterLayout = document.getElementById('filterLayout');
        for (const b of document.querySelectorAll('button')) {
            if (filterLayout && filterLayout.contains(b)) continue;
            const oc = b.getAttribute('onclick') || '';
            if (oc.includes('VhCatg') && oc.includes('combTablePnl')) {
                b.click(); return;
            }
        }
        throw new Error('Main refresh button not found');
    });
    await waitForAjax(page);
}

// ─── CLICK SIDEBAR REFRESH ────────────────────────────────────────────────────
// Find dynamically: any button inside #filterLayout whose onclick updates combTablePnl
async function clickSidebarRefresh(page) {
    await page.evaluate(() => {
        const sidebar = document.getElementById('filterLayout');
        if (!sidebar) throw new Error('filterLayout not found');
        const btn = Array.from(sidebar.querySelectorAll('button')).find(b =>
            (b.getAttribute('onclick') || '').includes('combTablePnl')
        );
        if (!btn) throw new Error('Sidebar refresh button not found');
        btn.click();
    });
    await waitForAjax(page);
}

// ─── SIDEBAR OPEN ─────────────────────────────────────────────────────────────
async function ensureSidebarOpen(page) {
    const isClosed = await page.evaluate(() => {
        const t = document.getElementById('filterLayout-toggler');
        return t && t.classList.contains('ui-layout-toggler-closed');
    });
    if (isClosed) {
        console.log('    Opening sidebar…');
        await page.evaluate(() => {
            document.getElementById('filterLayout-toggler').click();
        });
        await sleep(STEP_DELAY_MS);
    }
}

// ─── VEHICLE CLASS SELECT ─────────────────────────────────────────────────────
async function selectOneVehicleClass(page, vcIdx) {
    await page.evaluate((targetIdx) => {
        const inputs = document.querySelectorAll('input[name="VhClass"]');
        inputs.forEach((input, i) => {
            const box = input.closest('.ui-chkbox') && input.closest('.ui-chkbox').querySelector('.ui-chkbox-box');
            if (!box) return;
            const isChecked = input.checked;
            const shouldBeChecked = (i === targetIdx);
            if (isChecked !== shouldBeChecked) box.click();
        });
    }, vcIdx);
    await sleep(STEP_DELAY_MS);
}

// ─── EXPORT EXCEL ─────────────────────────────────────────────────────────────
async function downloadExcel(page, destPath) {
    const btnExists = await page.evaluate(() =>
        !!document.querySelector('img[title="Download EXCEL file"]')
    );
    if (!btnExists) throw new Error('Export button (Download EXCEL file) not found in DOM');

    const downloadPromise = page.waitForEvent('download', { timeout: 60_000 });

    await page.evaluate(() => {
        const img = document.querySelector('img[title="Download EXCEL file"]');
        if (!img) throw new Error('Export button not found');
        const btn = img.closest('a');
        if (!btn) throw new Error('Export anchor not found');
        btn.click();
    });

    await sleep(STEP_DELAY_MS);
    const download = await downloadPromise;
    await download.saveAs(destPath);
    await download.delete(); // remove Playwright's GUID temp file
    console.log(`     Saved: ${path.basename(destPath)}`);
}

// ─── PAGE RECOVERY ───────────────────────────────────────────────────────────
// Called when the page navigates away mid-run (e.g. download timeout causes reload).
// Reloads the page and restores all filter state so the retry can continue.
async function recoverPage(page, state, rto) {
    console.log('\n  [RECOVERY] Page lost — reloading and restoring state…');
    await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT_MS });
    await page.waitForFunction(
        () => window.PrimeFaces && Object.keys(PrimeFaces.widgets).length > 5,
        { timeout: 30_000 }
    );
    await sleep(STEP_DELAY_MS);

    await pfSelect(page, 'widget_yaxisVar', 'Maker');
    await pfSelect(page, 'widget_xaxisVar', 'Month Wise');
    await pfSelect(page, 'widget_selectedYearType', 'C');
    await pfSelect(page, 'widget_selectedYear', '2026');

    const newStateKey = await getStateWidgetKey(page);
    await pfSelect(page, newStateKey, state.code);
    await sleep(STEP_DELAY_MS);

    await page.waitForFunction(() => {
        const sel = document.getElementById('selectedRto_input');
        return sel && sel.options.length > 1;
    }, { timeout: 20_000 });

    await pfSelect(page, 'widget_selectedRto', rto.value);
    await clickMainRefresh(page);
    await ensureSidebarOpen(page);
    console.log('  [RECOVERY] Done ✓');
    return newStateKey;
}

// ─── KEEP-ALIVE ───────────────────────────────────────────────────────────────
let lastKeepAlive = Date.now();
async function keepAliveIfNeeded(page) {
    if (Date.now() - lastKeepAlive < KEEPALIVE_MS) return;
    lastKeepAlive = Date.now();
    console.log('  [KEEP-ALIVE] Pinging session…');
    try {
        await page.evaluate(() => {
            const nav = document.querySelector('nav.navbar');
            if (nav) nav.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        });
    } catch (_) { }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────
async function main() {
    fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

    await db.initDb();
    console.log('DB ready ✓');

    const [STATES, VEHICLE_CLASSES, done] = await Promise.all([
        db.loadStates(),
        db.loadVehicleClasses(),
        db.loadCompleted(),
    ]);
    console.log(`States: ${STATES.length}, Vehicle classes: ${VEHICLE_CLASSES.length}`);
    console.log(`Fetch progress: ${done.size} combos already done.\n`);

    const browser = await chromium.launch({
        headless: true,
        slowMo: 50,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        downloadsPath: DOWNLOAD_DIR,
    });

    const context = await browser.newContext({
        acceptDownloads: true,
        viewport: { width: 1366, height: 768 },
    });

    const page = await context.newPage();
    page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
    page.setDefaultTimeout(30_000);

    // ── 1. Load page ──────────────────────────────────────────────────────────
    await withRetry(async () => {
        await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT_MS });
        await page.waitForSelector('#filterLayout-toggler', { state: 'attached', timeout: 30_000 });
        await page.waitForFunction(
            () => window.PrimeFaces && Object.keys(PrimeFaces.widgets).length > 5,
            { timeout: 30_000 }
        );
        await sleep(STEP_DELAY_MS);
        console.log('Page loaded ✓');
    }, 'Page load');

    // ── 2. One-time axis/year config ──────────────────────────────────────────
    await withRetry(async () => {
        await pfSelect(page, 'widget_yaxisVar', 'Maker');
        console.log('Y-Axis = Maker ✓');

        await pfSelect(page, 'widget_xaxisVar', 'Month Wise');
        console.log('X-Axis = Month Wise ✓');

        await pfSelect(page, 'widget_selectedYearType', 'C');
        console.log('Year Type = Calendar Year ✓');

        await pfSelect(page, 'widget_selectedYear', '2026');
        console.log('Year = 2026 ✓');
    }, 'Axis/year setup');

    // ── 3. Find dynamic state widget key ──────────────────────────────────────
    let stateWidgetKey = await getStateWidgetKey(page);
    if (!stateWidgetKey) throw new Error('Cannot find state dropdown widget');
    console.log(`State widget key: ${stateWidgetKey}\n`);

    // ── 4. Open sidebar once ──────────────────────────────────────────────────
    await ensureSidebarOpen(page);
    console.log('Sidebar open ✓\n');

    // ── 5. Main loop ──────────────────────────────────────────────────────────
    for (const state of STATES) {
        console.log(`\n${'═'.repeat(60)}`);
        console.log(`STATE: ${state.name} (${state.code})`);
        console.log('═'.repeat(60));

        await withRetry(async () => {
            stateWidgetKey = await getStateWidgetKey(page) || stateWidgetKey;
            await pfSelect(page, stateWidgetKey, state.code);
            await sleep(STEP_DELAY_MS);
            console.log(`  State selected. Waiting for RTOs…`);
        }, `Select state ${state.name}`);

        await page.waitForFunction(() => {
            const sel = document.getElementById('selectedRto_input');
            return sel && sel.options.length > 1;
        }, { timeout: 20_000 });
        await sleep(STEP_DELAY_MS);
        console.log(`  RTOs ready.`);

        const rtos = await page.evaluate(() => {
            const sel = document.getElementById('selectedRto_input');
            return Array.from(sel.options)
                .filter(o => o.value !== '-1')
                .map(o => ({ value: o.value, text: o.text.split('(')[0].trim() }));
        });
        console.log(`  ${rtos.length} RTOs found.`);

        await db.saveRtos(state.code, rtos);

        for (const rto of rtos) {
            console.log(`\n  ── RTO: ${rto.text} (${rto.value})`);

            await withRetry(async () => {
                await pfSelect(page, 'widget_selectedRto', rto.value);
                await sleep(STEP_DELAY_MS);
            }, `Select RTO ${rto.text}`);

            await withRetry(async () => {
                await clickMainRefresh(page);
                await sleep(STEP_DELAY_MS);
                console.log(`     Main refresh done`);
            }, `Main refresh ${state.code}/${rto.value}`);

            await ensureSidebarOpen(page);

            for (const vc of VEHICLE_CLASSES) {
                const key = comboKey(state.code, rto.value, vc.idx);
                if (done.has(key)) {
                    process.stdout.write('·');
                    continue;
                }

                await keepAliveIfNeeded(page);

                try {
                    await withRetry(async () => {
                        // If page navigated away, reload and restore before retrying
                        const pageGone = await page.evaluate(() =>
                            !document.getElementById('filterLayout') || !window.PrimeFaces
                        ).catch(() => true);
                        if (pageGone) {
                            stateWidgetKey = await recoverPage(page, state, rto);
                        }

                        await selectOneVehicleClass(page, vc.idx);
                        await clickSidebarRefresh(page);
                        await sleep(STEP_DELAY_MS);

                        // Check only the main table panel — dropdown options contain RTO codes like AP502/AP503
                        // which would false-positive on document.body.innerText
                        const hasError = await page.evaluate(() => {
                            const panel = document.getElementById('combTablePnl') || document.body;
                            return /Service Unavailable|Internal Server Error/i.test(panel.innerText);
                        });
                        if (hasError) throw new Error('Server error after sidebar refresh');

                        const safeName = `${state.code}__${rto.value}__${String(vc.idx).padStart(2, '0')}__${vc.label}`;
                        const destPath = path.join(DOWNLOAD_DIR, `${safeName}.xls`);

                        if (fs.existsSync(destPath)) {
                            console.log(`\n     [EXISTS] ${safeName}.xls`);
                            await db.logFetch(state.code, rto.value, vc.idx, 'skipped', 'file already exists');
                        } else {
                            await db.logFetch(state.code, rto.value, vc.idx, 'started', null);
                            await downloadExcel(page, destPath);
                            await sleep(STEP_DELAY_MS);
                            const rows = await db.processXlsFile(destPath, `${safeName}.xls`);
                            await db.logFetch(state.code, rto.value, vc.idx, 'success', `${rows} rows inserted`);
                        }

                        done.add(key);
                        await db.markCompleted(state.code, rto.value, vc.idx);

                    }, `Export ${state.code}/${rto.value}/vc${vc.idx}:${vc.label}`);
                } catch (err) {
                    const msg = err.message?.split('\n')[0];
                    console.warn(`\n     [FAILED] ${state.code}/${rto.value}/vc${vc.idx} — ${msg}`);
                    await db.logFetch(state.code, rto.value, vc.idx, 'error', msg);
                    // skip this combo and continue — do NOT re-throw
                }
            }

            console.log();
        }
    }

    console.log('\nAll combinations completed!');
    await browser.close();
    await db.closeDb();
}

main().catch(async (err) => {
    console.error('\nFatal:', err.message);
    await db.closeDb().catch(() => { });
    process.exit(1);
});
