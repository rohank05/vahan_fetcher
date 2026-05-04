// recheck.js — Find state+RTO+year combos where fetch_progress is marked done
// but total registration count is 0, then reset so the scraper re-downloads.
//
// Usage:
//   node recheck.js                    — dry run, shows what would be reset
//   node recheck.js --fix              — reset all
//   node recheck.js --fix --year=2026  — reset only for a specific year
//   node recheck.js --fix --state=UK   — reset only for a specific state

require('dotenv').config();
const db = require('./db');

const DRY_RUN  = !process.argv.includes('--fix');
const yearArg  = (process.argv.find(a => a.startsWith('--year='))  || '').replace('--year=',  '');
const stateArg = (process.argv.find(a => a.startsWith('--state=')) || '').replace('--state=', '');

async function main() {
    await db.initDb();
    const { pool } = db;

    const whereConditions = [];
    if (yearArg)  whereConditions.push(`fp.year = ${Number.parseInt(yearArg)}`);
    if (stateArg) whereConditions.push(`s.code = '${stateArg.toUpperCase()}'`);
    const WHERE = whereConditions.length ? `AND ${whereConditions.join(' AND ')}` : '';

    // Find (state, RTO, year) groups where all vehicle class combos have 0 registrations
    const { rows: empty } = await pool.query(`
        SELECT
            s.code   AS state_code,
            r.code   AS rto_code,
            fp.year,
            COUNT(fp.id)            AS fetch_entries,
            COALESCE(SUM(vr.count), 0) AS total_registrations
        FROM fetch_progress fp
        JOIN states s ON s.id = fp.state_id
        JOIN rtos   r ON r.id = fp.rto_id
        LEFT JOIN vehicle_registrations vr
            ON  vr.state_id         = fp.state_id
            AND vr.rto_id           = fp.rto_id
            AND vr.vehicle_class_id = fp.vehicle_class_id
            AND vr.year             = fp.year
        WHERE 1=1 ${WHERE}
        GROUP BY s.code, fp.state_id, r.code, fp.rto_id, fp.year
        HAVING COALESCE(SUM(vr.count), 0) = 0
        ORDER BY s.code, fp.year, r.code
    `);

    if (empty.length === 0) {
        console.log('No empty RTO+year combos found — everything looks good.');
        await db.closeDb();
        return;
    }

    // Summarise by state+year
    const summary = {};
    for (const row of empty) {
        const key = `${row.state_code}|${row.year}`;
        if (!summary[key]) summary[key] = { state: row.state_code, year: row.year, rtos: 0, entries: 0 };
        summary[key].rtos++;
        summary[key].entries += Number(row.fetch_entries);
    }

    console.log(`Found ${empty.length} RTO+year pair(s) with 0 registrations:\n`);
    for (const { state, year, rtos, entries } of Object.values(summary)) {
        console.log(`  ${state} [year=${year}] — ${rtos} RTOs, ${entries} fetch_progress entries to reset`);
    }

    if (DRY_RUN) {
        console.log(`\nDry run — run with --fix to reset these for re-download.`);
        console.log(`Options: --year=2026  --state=UK  (can be combined with --fix)`);
        await db.closeDb();
        return;
    }

    // Delete fetch_progress entries for all identified (state, rto, year) combos
    const stateRtoYears = empty.map(r => `('${r.state_code}', '${r.rto_code}', ${r.year})`).join(', ');
    const { rowCount } = await pool.query(`
        DELETE FROM fetch_progress fp
        USING states s, rtos r
        WHERE s.id = fp.state_id
          AND r.id = fp.rto_id
          AND (s.code, r.code, fp.year) IN (${stateRtoYears})
    `);

    console.log(`\nReset ${rowCount} fetch_progress entries — they will be re-downloaded on next scraper run.`);
    await db.closeDb();
}

main().catch(async err => {
    console.error('Fatal:', err.message);
    await db.closeDb().catch(() => {});
    process.exit(1);
});
