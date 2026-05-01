// processor.js — Process any leftover XLS files not yet inserted into the DB
// node processor.js

const fs = require('fs');
const path = require('path');
const db = require('./db');

const DOWNLOAD_DIR = path.resolve('./vahan_downloads');

async function main() {
    await db.initDb();
    await db.loadStates();
    await db.loadVehicleClasses();

    const doneSet = await db.loadProcessedFiles();

    const files = fs.readdirSync(DOWNLOAD_DIR)
        .filter(f => f.endsWith('.xls'))
        .filter(f => !doneSet.has(f))
        .sort();

    console.log(`Files to process: ${files.length}  (${doneSet.size} already done)\n`);

    let totalRows = 0, skipped = 0, errors = 0;

    for (const f of files) {
        process.stdout.write(`  ${f} … `);
        try {
            const n = await db.processXlsFile(path.join(DOWNLOAD_DIR, f), f);
            if (n === 0) { process.stdout.write('(empty)\n'); skipped++; }
            else { process.stdout.write(`${n} rows\n`); totalRows += n; }
        } catch (err) {
            process.stdout.write(`ERROR: ${err.message}\n`);
            errors++;
        }
    }

    console.log(`\nDone.`);
    console.log(`  Rows inserted : ${totalRows}`);
    console.log(`  Empty files   : ${skipped}`);
    console.log(`  Errors        : ${errors}`);

    await db.closeDb();
}

main().catch(async err => {
    console.error('Fatal:', err.message);
    await db.closeDb().catch(() => {});
    process.exit(1);
});
