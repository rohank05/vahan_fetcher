// recheck.js — Find states where fetch_progress is marked done but total
// registration count is 0 for that year, then reset so scraper re-downloads.
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

    const havingConditions = ['COALESCE(SUM(vr.count), 0) = 0'];
    if (yearArg)  havingConditions.push(`fp.year = ${parseInt(yearArg)}`);
    if (stateArg) havingConditions.push(`s.code = '${stateArg.toUpperCase()}'`);

    // Find state+year combos that are fully done in fetch_progress but have 0 registrations
    const { rows: empty } = await pool.query(`
        SELECT
            s.code AS state_code,
            fp.year,
            COUNT(fp.id) AS combos
        FROM fetch_progress fp
        JOIN states s ON s.id = fp.state_id
        LEFT JOIN vehicle_registrations vr
            ON  vr.state_id         = fp.state_id
            AND vr.rto_id           = fp.rto_id
            AND vr.vehicle_class_id = fp.vehicle_class_id
            AND vr.year             = fp.year
        GROUP BY s.code, fp.state_id, fp.year
        HAVING ${havingConditions.join(' AND ')}
        ORDER BY s.code, fp.year
    `);

    if (empty.length === 0) {
        console.log('No empty state+year combos found — everything looks good.');
        await db.closeDb();
        return;
    }

    console.log(`Found ${empty.length} state+year pair(s) with 0 registrations:\n`);
    for (const row of empty) {
        console.log(`  ${row.state_code} [year=${row.year}] — ${row.combos} fetch_progress entries to reset`);
    }

    if (DRY_RUN) {
        console.log(`\nDry run — run with --fix to reset these for re-download.`);
        console.log(`Options: --year=2026  --state=UK  (can be combined with --fix)`);
        await db.closeDb();
        return;
    }

    let total = 0;
    for (const row of empty) {
        const { rowCount } = await pool.query(`
            DELETE FROM fetch_progress fp
            USING states s
            WHERE s.id = fp.state_id
              AND s.code = $1
              AND fp.year = $2
        `, [row.state_code, row.year]);
        console.log(`  Deleted ${rowCount} entries for ${row.state_code} ${row.year}`);
        total += rowCount;
    }

    console.log(`\nReset ${total} fetch_progress entries — they will be re-downloaded on next scraper run.`);
    await db.closeDb();
}

main().catch(async err => {
    console.error('Fatal:', err.message);
    await db.closeDb().catch(() => {});
    process.exit(1);
});
