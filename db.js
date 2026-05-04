require('dotenv').config();
const { Pool } = require('pg');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');

const pool = new Pool({
    host:     process.env.PG_HOST     || 'localhost',
    port:     parseInt(process.env.PG_PORT || '5432'),
    user:     process.env.PG_USER     || 'postgres',
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DATABASE,
});

// ─── IN-MEMORY CACHE ─────────────────────────────────────────────────────────
const cache = {
    stateByCode:  new Map(),   // code  -> { id, code, name }
    vcByIdx:      new Map(),   // idx   -> { id, idx, label }
    rtoByKey:     new Map(),   // `${stateId}:${rtoCode}` -> id
    makerByName:  new Map(),   // name  -> id
};

// ─── SEED DATA ────────────────────────────────────────────────────────────────

const STATES_SEED = [
    { code: 'AN', name: 'Andaman & Nicobar Island' },
    { code: 'AP', name: 'Andhra Pradesh' },
    { code: 'AR', name: 'Arunachal Pradesh' },
    { code: 'AS', name: 'Assam' },
    { code: 'BR', name: 'Bihar' },
    { code: 'CG', name: 'Chhattisgarh' },
    { code: 'CH', name: 'Chandigarh' },
    { code: 'DD', name: 'UT of DNH and DD' },
    { code: 'DL', name: 'Delhi' },
    { code: 'GA', name: 'Goa' },
    { code: 'GJ', name: 'Gujarat' },
    { code: 'HP', name: 'Himachal Pradesh' },
    { code: 'HR', name: 'Haryana' },
    { code: 'JH', name: 'Jharkhand' },
    { code: 'JK', name: 'Jammu and Kashmir' },
    { code: 'KA', name: 'Karnataka' },
    { code: 'KL', name: 'Kerala' },
    { code: 'LA', name: 'Ladakh' },
    { code: 'LD', name: 'Lakshadweep' },
    { code: 'MH', name: 'Maharashtra' },
    { code: 'ML', name: 'Meghalaya' },
    { code: 'MN', name: 'Manipur' },
    { code: 'MP', name: 'Madhya Pradesh' },
    { code: 'MZ', name: 'Mizoram' },
    { code: 'NL', name: 'Nagaland' },
    { code: 'OR', name: 'Odisha' },
    { code: 'PB', name: 'Punjab' },
    { code: 'PY', name: 'Puducherry' },
    { code: 'RJ', name: 'Rajasthan' },
    { code: 'SK', name: 'Sikkim' },
    { code: 'TG', name: 'Telangana' },
    { code: 'TN', name: 'Tamil Nadu' },
    { code: 'TR', name: 'Tripura' },
    { code: 'UK', name: 'Uttarakhand' },
    { code: 'UP', name: 'Uttar Pradesh' },
    { code: 'WB', name: 'West Bengal' },
];

const VEHICLE_CLASSES_SEED = [
    { idx: 0,  label: 'M-CYCLE/SCOOTER',                              alias: 'M-CYCLE_SCOOTER',                       websiteId: 1   },
    { idx: 1,  label: 'M-CYCLE/SCOOTER-WITH SIDE CAR',                alias: 'M-CYCLE_SCOOTER-WITH_SIDE_CAR',         websiteId: 2   },
    { idx: 2,  label: 'MOPED',                                         alias: 'MOPED',                                 websiteId: 3   },
    { idx: 3,  label: 'MOTORISED CYCLE (CC > 25CC)',                   alias: 'MOTORISED_CYCLE',                       websiteId: 4   },
    { idx: 4,  label: 'ADAPTED VEHICLE',                               alias: 'ADAPTED_VEHICLE',                       websiteId: 5   },
    { idx: 5,  label: 'THREE WHEELER (PERSONAL)',                      alias: 'THREE_WHEELER_PERSONAL',                websiteId: 6   },
    { idx: 6,  label: 'MOTOR CAR',                                     alias: 'MOTOR_CAR',                             websiteId: 7   },
    { idx: 7,  label: 'FORK LIFT',                                     alias: 'FORK_LIFT',                             websiteId: 8   },
    { idx: 8,  label: 'VEHICLE FITTED WITH RIG',                       alias: 'VEHICLE_FITTED_WITH_RIG',               websiteId: 9   },
    { idx: 9,  label: 'VEHICLE FITTED WITH GENERATOR',                 alias: 'VEHICLE_FITTED_WITH_GENERATOR',         websiteId: 10  },
    { idx: 10, label: 'VEHICLE FITTED WITH COMPRESSOR',                alias: 'VEHICLE_FITTED_WITH_COMPRESSOR',        websiteId: 11  },
    { idx: 11, label: 'CRANE MOUNTED VEHICLE',                         alias: 'CRANE_MOUNTED_VEHICLE',                 websiteId: 12  },
    { idx: 12, label: 'AGRICULTURAL TRACTOR',                          alias: 'AGRICULTURAL_TRACTOR',                  websiteId: 13  },
    { idx: 13, label: 'POWER TILLER',                                  alias: 'POWER_TILLER',                          websiteId: 14  },
    { idx: 14, label: 'PRIVATE SERVICE VEHICLE (INDIVIDUAL USE)',       alias: 'PRIVATE_SERVICE_VEHICLE_INDIVIDUAL',    websiteId: 15  },
    { idx: 15, label: 'CAMPER VAN / TRAILER (PRIVATE USE)',            alias: 'CAMPER_VAN_TRAILER_PRIVATE',            websiteId: 16  },
    { idx: 16, label: 'TOW TRUCK',                                     alias: 'TOW_TRUCK',                             websiteId: 17  },
    { idx: 17, label: 'BREAKDOWN VAN',                                 alias: 'BREAKDOWN_VAN',                         websiteId: 18  },
    { idx: 18, label: 'RECOVERY VEHICLE',                              alias: 'RECOVERY_VEHICLE',                      websiteId: 19  },
    { idx: 19, label: 'TOWER WAGON',                                   alias: 'TOWER_WAGON',                           websiteId: 20  },
    { idx: 20, label: 'TREE TRIMMING VEHICLE',                         alias: 'TREE_TRIMMING_VEHICLE',                 websiteId: 21  },
    { idx: 21, label: 'CONSTRUCTION EQUIPMENT VEHICLE',                alias: 'CONSTRUCTION_EQUIPMENT_VEHICLE',        websiteId: 22  },
    { idx: 22, label: 'OMNI BUS (PRIVATE USE)',                        alias: 'OMNI_BUS_PRIVATE',                      websiteId: 23  },
    { idx: 23, label: 'ROAD ROLLER',                                   alias: 'ROAD_ROLLER',                           websiteId: 24  },
    { idx: 24, label: 'EXCAVATOR (NT)',                                 alias: 'EXCAVATOR_NT',                          websiteId: 25  },
    { idx: 25, label: 'BULLDOZER',                                     alias: 'BULLDOZER',                             websiteId: 26  },
    { idx: 26, label: 'HARVESTER',                                     alias: 'HARVESTER',                             websiteId: 27  },
    { idx: 27, label: 'TRAILER (AGRICULTURAL)',                        alias: 'TRAILER_AGRICULTURAL',                  websiteId: 28  },
    { idx: 28, label: 'EARTH MOVING EQUIPMENT',                        alias: 'EARTH_MOVING_EQUIPMENT',                websiteId: 29  },
    { idx: 29, label: 'TRAILER FOR PERSONAL USE',                      alias: 'TRAILER_PERSONAL_USE',                  websiteId: 30  },
    { idx: 30, label: 'QUADRICYCLE (PRIVATE)',                         alias: 'QUADRICYCLE_PRIVATE',                   websiteId: 31  },
    { idx: 31, label: 'ARMOURED/SPECIALISED VEHICLE',                  alias: 'ARMOURED_SPECIALISED_VEHICLE',          websiteId: 32  },
    { idx: 32, label: 'MOTOR CARAVAN',                                 alias: 'MOTOR_CARAVAN',                         websiteId: 33  },
    { idx: 33, label: 'MOTOR CYCLE/SCOOTER-SIDECAR(T)',                alias: 'MOTOR_CYCLE_SCOOTER_SIDECAR_T',         websiteId: 51  },
    { idx: 34, label: 'MOTOR CYCLE/SCOOTER-WITH TRAILER',              alias: 'MOTOR_CYCLE_SCOOTER_WITH_TRAILER',      websiteId: 52  },
    { idx: 35, label: 'MOTOR CYCLE/SCOOTER-USED FOR HIRE',             alias: 'MOTOR_CYCLE_SCOOTER_USED_FOR_HIRE',     websiteId: 53  },
    { idx: 36, label: 'E-RICKSHAW WITH CART (G)',                      alias: 'E-RICKSHAW_WITH_CART_G',                websiteId: 54  },
    { idx: 37, label: 'E-RICKSHAW(P)',                                  alias: 'E-RICKSHAW_P',                          websiteId: 55  },
    { idx: 38, label: 'LUXURY CAB',                                    alias: 'LUXURY_CAB',                            websiteId: 56  },
    { idx: 39, label: 'THREE WHEELER (PASSENGER)',                     alias: 'THREE_WHEELER_PASSENGER',               websiteId: 57  },
    { idx: 40, label: 'THREE WHEELER (GOODS)',                         alias: 'THREE_WHEELER_GOODS',                   websiteId: 58  },
    { idx: 41, label: 'GOODS CARRIER',                                 alias: 'GOODS_CARRIER',                         websiteId: 59  },
    { idx: 42, label: 'POWER TILLER (COMMERCIAL)',                     alias: 'POWER_TILLER_COMMERCIAL',               websiteId: 62  },
    { idx: 43, label: 'TRACTOR (COMMERCIAL)',                          alias: 'TRACTOR_COMMERCIAL',                    websiteId: 63  },
    { idx: 44, label: 'MOBILE CLINIC',                                 alias: 'MOBILE_CLINIC',                         websiteId: 64  },
    { idx: 45, label: 'X-RAY VAN',                                     alias: 'X-RAY_VAN',                             websiteId: 65  },
    { idx: 46, label: 'LIBRARY VAN',                                   alias: 'LIBRARY_VAN',                           websiteId: 66  },
    { idx: 47, label: 'MOBILE WORKSHOP',                               alias: 'MOBILE_WORKSHOP',                       websiteId: 67  },
    { idx: 48, label: 'MOBILE CANTEEN',                                alias: 'MOBILE_CANTEEN',                        websiteId: 68  },
    { idx: 49, label: 'PRIVATE SERVICE VEHICLE',                       alias: 'PRIVATE_SERVICE_VEHICLE',               websiteId: 69  },
    { idx: 50, label: 'MAXI CAB',                                      alias: 'MAXI_CAB',                              websiteId: 70  },
    { idx: 51, label: 'MOTOR CAB',                                     alias: 'MOTOR_CAB',                             websiteId: 71  },
    { idx: 52, label: 'PULLER TRACTOR',                                alias: 'PULLER_TRACTOR',                        websiteId: 72  },
    { idx: 53, label: 'BUS',                                           alias: 'BUS',                                   websiteId: 73  },
    { idx: 54, label: 'SCHOOL BUS',                                    alias: 'SCHOOL_BUS',                            websiteId: 74  },
    { idx: 55, label: 'EDUCATIONAL INSTITUTION BUS',                   alias: 'EDUCATIONAL_INSTITUTION_BUS',           websiteId: 75  },
    { idx: 56, label: 'AMBULANCE',                                     alias: 'AMBULANCE',                             websiteId: 76  },
    { idx: 57, label: 'ANIMAL AMBULANCE',                              alias: 'ANIMAL_AMBULANCE',                      websiteId: 77  },
    { idx: 58, label: 'CAMPER VAN / TRAILER',                          alias: 'CAMPER_VAN_TRAILER',                    websiteId: 78  },
    { idx: 59, label: 'CASH VAN',                                      alias: 'CASH_VAN',                              websiteId: 79  },
    { idx: 60, label: 'FIRE TENDERS',                                  alias: 'FIRE_TENDERS',                          websiteId: 80  },
    { idx: 61, label: 'SNORKED LADDERS',                               alias: 'SNORKED_LADDERS',                       websiteId: 81  },
    { idx: 62, label: 'AUXILIARY TRAILER',                             alias: 'AUXILIARY_TRAILER',                     websiteId: 82  },
    { idx: 63, label: 'FIRE FIGHTING VEHICLE',                         alias: 'FIRE_FIGHTING_VEHICLE',                 websiteId: 83  },
    { idx: 64, label: 'ARTICULATED VEHICLE',                           alias: 'ARTICULATED_VEHICLE',                   websiteId: 84  },
    { idx: 65, label: 'HEARSES',                                       alias: 'HEARSES',                               websiteId: 85  },
    { idx: 66, label: 'OMNI BUS',                                      alias: 'OMNI_BUS',                              websiteId: 86  },
    { idx: 67, label: 'DUMPER',                                        alias: 'DUMPER',                                websiteId: 87  },
    { idx: 68, label: 'EXCAVATOR (COMMERCIAL)',                        alias: 'EXCAVATOR_COMMERCIAL',                  websiteId: 88  },
    { idx: 69, label: 'TRAILER (COMMERCIAL)',                          alias: 'TRAILER_COMMERCIAL',                    websiteId: 89  },
    { idx: 70, label: 'TRACTOR-TROLLEY (COMMERCIAL)',                  alias: 'TRACTOR_TROLLEY_COMMERCIAL',            websiteId: 90  },
    { idx: 71, label: 'SEMI-TRAILER (COMMERCIAL)',                     alias: 'SEMI_TRAILER_COMMERCIAL',               websiteId: 91  },
    { idx: 72, label: 'CONSTRUCTION EQUIPMENT VEHICLE (COMMERCIAL)',   alias: 'CONSTRUCTION_EQUIPMENT_COMMERCIAL',     websiteId: 92  },
    { idx: 73, label: 'QUADRICYCLE (COMMERCIAL)',                      alias: 'QUADRICYCLE_COMMERCIAL',                websiteId: 93  },
    { idx: 74, label: 'MODULAR HYDRAULIC TRAILER',                     alias: 'MODULAR_HYDRAULIC_TRAILER',             websiteId: 94  },
    { idx: 75, label: 'VINTAGE MOTOR VEHICLE',                         alias: 'VINTAGE_MOTOR_VEHICLE',                 websiteId: 157 },
];

// ─── SCHEMA ───────────────────────────────────────────────────────────────────

async function initDb() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS states (
            id    SERIAL PRIMARY KEY,
            code  VARCHAR(5) UNIQUE NOT NULL,
            name  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS vehicle_classes (
            id         SERIAL PRIMARY KEY,
            idx        INTEGER UNIQUE NOT NULL,
            label      TEXT NOT NULL,
            alias      TEXT,
            website_id INTEGER
        );
        ALTER TABLE vehicle_classes ADD COLUMN IF NOT EXISTS alias      TEXT;
        ALTER TABLE vehicle_classes ADD COLUMN IF NOT EXISTS website_id INTEGER;

        CREATE TABLE IF NOT EXISTS rtos (
            id        SERIAL PRIMARY KEY,
            code      TEXT NOT NULL,
            state_id  INTEGER NOT NULL REFERENCES states(id),
            name      TEXT NOT NULL,
            UNIQUE (code, state_id)
        );

        CREATE INDEX IF NOT EXISTS idx_rtos_state ON rtos(state_id);

        CREATE TABLE IF NOT EXISTS makers (
            id   SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_makers_name ON makers(name);

        CREATE TABLE IF NOT EXISTS vehicle_registrations (
            id               BIGSERIAL PRIMARY KEY,
            state_id         INTEGER NOT NULL REFERENCES states(id),
            rto_id           INTEGER NOT NULL REFERENCES rtos(id),
            vehicle_class_id INTEGER NOT NULL REFERENCES vehicle_classes(id),
            maker_id         INTEGER NOT NULL REFERENCES makers(id),
            year             INTEGER NOT NULL,
            month            INTEGER NOT NULL,
            count            INTEGER NOT NULL DEFAULT 0,
            created_at       TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE (state_id, rto_id, vehicle_class_id, maker_id, year, month)
        );

        CREATE INDEX IF NOT EXISTS idx_vr_state      ON vehicle_registrations(state_id);
        CREATE INDEX IF NOT EXISTS idx_vr_rto        ON vehicle_registrations(rto_id);
        CREATE INDEX IF NOT EXISTS idx_vr_maker      ON vehicle_registrations(maker_id);
        CREATE INDEX IF NOT EXISTS idx_vr_vc         ON vehicle_registrations(vehicle_class_id);
        CREATE INDEX IF NOT EXISTS idx_vr_year_month ON vehicle_registrations(year, month);
        CREATE INDEX IF NOT EXISTS idx_vr_state_year ON vehicle_registrations(state_id, year, month);

        CREATE TABLE IF NOT EXISTS fetch_progress (
            id               SERIAL PRIMARY KEY,
            state_id         INTEGER NOT NULL REFERENCES states(id),
            rto_id           INTEGER NOT NULL REFERENCES rtos(id),
            vehicle_class_id INTEGER NOT NULL REFERENCES vehicle_classes(id),
            year             INTEGER NOT NULL DEFAULT 2026,
            completed_at     TIMESTAMPTZ DEFAULT NOW(),
            CONSTRAINT fetch_progress_unique UNIQUE (state_id, rto_id, vehicle_class_id, year)
        );

        CREATE TABLE IF NOT EXISTS fetch_logs (
            id               BIGSERIAL PRIMARY KEY,
            state_id         INTEGER REFERENCES states(id),
            rto_id           INTEGER REFERENCES rtos(id),
            vehicle_class_id INTEGER REFERENCES vehicle_classes(id),
            status           TEXT NOT NULL,
            message          TEXT,
            created_at       TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_fl_created ON fetch_logs(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_fl_status  ON fetch_logs(status);

        CREATE TABLE IF NOT EXISTS processed_files (
            id           SERIAL PRIMARY KEY,
            filename     TEXT UNIQUE NOT NULL,
            row_count    INTEGER NOT NULL DEFAULT 0,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        );
    `);

    // Migrate fetch_progress: add year column if missing and replace unique constraint
    await pool.query(`
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'fetch_progress' AND column_name = 'year'
            ) THEN
                ALTER TABLE fetch_progress ADD COLUMN year INTEGER NOT NULL DEFAULT 2026;
                ALTER TABLE fetch_progress DROP CONSTRAINT IF EXISTS fetch_progress_state_id_rto_id_vehicle_class_id_key;
                ALTER TABLE fetch_progress ADD CONSTRAINT fetch_progress_unique
                    UNIQUE (state_id, rto_id, vehicle_class_id, year);
            END IF;
        END $$;
    `);

    await seedStates();
    await seedVehicleClasses();
}

// ─── SEED ─────────────────────────────────────────────────────────────────────

async function seedStates() {
    const { rows } = await pool.query('SELECT COUNT(*) AS c FROM states');
    if (parseInt(rows[0].c) > 0) return;
    for (const s of STATES_SEED) {
        await pool.query(
            'INSERT INTO states (code, name) VALUES ($1, $2) ON CONFLICT DO NOTHING',
            [s.code, s.name]
        );
    }
    console.log(`Seeded ${STATES_SEED.length} states.`);
}

async function seedVehicleClasses() {
    const { rows } = await pool.query('SELECT COUNT(*) AS c FROM vehicle_classes');
    if (parseInt(rows[0].c) > 0) {
        // Update alias/website_id on existing rows in case they were seeded before these columns existed
        for (const vc of VEHICLE_CLASSES_SEED) {
            await pool.query(
                `UPDATE vehicle_classes SET alias = $1, website_id = $2 WHERE idx = $3 AND (alias IS NULL OR website_id IS NULL)`,
                [vc.alias, vc.websiteId, vc.idx]
            );
        }
        return;
    }
    for (const vc of VEHICLE_CLASSES_SEED) {
        await pool.query(
            'INSERT INTO vehicle_classes (idx, label, alias, website_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING',
            [vc.idx, vc.label, vc.alias, vc.websiteId]
        );
    }
    console.log(`Seeded ${VEHICLE_CLASSES_SEED.length} vehicle classes.`);
}

// ─── CACHE LOADERS ───────────────────────────────────────────────────────────

async function loadStates() {
    const { rows } = await pool.query('SELECT id, code, name FROM states ORDER BY code');
    for (const r of rows) cache.stateByCode.set(r.code, r);
    return rows;
}

async function loadVehicleClasses() {
    const { rows } = await pool.query('SELECT id, idx, label, alias, website_id FROM vehicle_classes ORDER BY idx');
    for (const r of rows) cache.vcByIdx.set(r.idx, r);
    return rows;
}

async function loadCompleted() {
    const { rows } = await pool.query(`
        SELECT s.code AS state_code, r.code AS rto_code, vc.idx AS vc_idx, fp.year
        FROM fetch_progress fp
        JOIN states s           ON s.id  = fp.state_id
        JOIN rtos r             ON r.id  = fp.rto_id
        JOIN vehicle_classes vc ON vc.id = fp.vehicle_class_id
    `);
    return new Set(rows.map(r => `${r.state_code}|${r.rto_code}|${r.vc_idx}|${r.year}`));
}

// ─── RTO HELPERS ─────────────────────────────────────────────────────────────

async function saveRtos(stateCode, rtos) {
    const state = cache.stateByCode.get(stateCode);
    if (!state) return;
    for (const rto of rtos) {
        const { rows } = await pool.query(
            `INSERT INTO rtos (code, state_id, name) VALUES ($1, $2, $3)
             ON CONFLICT (code, state_id) DO UPDATE SET name = EXCLUDED.name
             RETURNING id`,
            [rto.value, state.id, rto.text]
        );
        cache.rtoByKey.set(`${state.id}:${rto.value}`, rows[0].id);
    }
}

async function getRtoId(stateCode, rtoCode) {
    const state = cache.stateByCode.get(stateCode);
    if (!state) return null;
    const key = `${state.id}:${rtoCode}`;
    if (cache.rtoByKey.has(key)) return cache.rtoByKey.get(key);
    const { rows } = await pool.query(
        'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
        [rtoCode, state.id]
    );
    if (!rows.length) return null;
    cache.rtoByKey.set(key, rows[0].id);
    return rows[0].id;
}

// ─── MAKER HELPERS ───────────────────────────────────────────────────────────

async function getOrCreateMaker(name) {
    if (cache.makerByName.has(name)) return cache.makerByName.get(name);
    const { rows } = await pool.query(
        `INSERT INTO makers (name) VALUES ($1)
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
         RETURNING id`,
        [name]
    );
    cache.makerByName.set(name, rows[0].id);
    return rows[0].id;
}

// ─── FETCH PROGRESS ───────────────────────────────────────────────────────────

async function markCompleted(stateCode, rtoCode, vcIdx, year) {
    const state = cache.stateByCode.get(stateCode);
    const vc    = cache.vcByIdx.get(vcIdx);
    const rtoId = await getRtoId(stateCode, rtoCode);
    if (!state || !vc || !rtoId) return;
    await pool.query(
        `INSERT INTO fetch_progress (state_id, rto_id, vehicle_class_id, year)
         VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
        [state.id, rtoId, vc.id, year]
    );
}

async function logFetch(stateCode, rtoCode, vcIdx, status, message) {
    const state = cache.stateByCode.get(stateCode);
    const vc    = cache.vcByIdx.get(vcIdx);
    const rtoId = rtoCode ? await getRtoId(stateCode, rtoCode) : null;
    await pool.query(
        `INSERT INTO fetch_logs (state_id, rto_id, vehicle_class_id, status, message)
         VALUES ($1, $2, $3, $4, $5)`,
        [state?.id ?? null, rtoId ?? null, vc?.id ?? null, status, message ?? null]
    );
}

// ─── XLS PARSER ───────────────────────────────────────────────────────────────

const MONTH_NAMES = {
    jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
    jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12,
};

// Filename format: STATE__RTO__IDX__ALIAS__YEAR.xls  (YEAR optional for old files)
function parseFilename(filename) {
    const base = path.basename(filename, '.xls');
    const parts = base.split('__');
    if (parts.length < 3) return null;
    const vcIdx = parseInt(parts[2]);
    if (isNaN(vcIdx)) return null;
    return { stateCode: parts[0], rtoCode: parts[1], vcIdx };
}

// Sheet layout (confirmed from sample):
//   Row 0: Title  "Maker Month Wise Data … (<Year>)"
//   Row 1: S No | Maker | [Month Wise merged] | TOTAL
//   Row 2: blank spacer
//   Row 3: ""  | ""    | JAN | FEB | … | ""
//   Row 4+: data rows
function parseSheet(ws) {
    const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    if (raw.length < 5) return null;

    const title = String(raw[0]?.[0] || '');
    const yearMatch = /\((\d{4})\)/.exec(title);
    const year = yearMatch ? parseInt(yearMatch[1]) : null;
    if (!year) return null;

    const monthHeaderRow = raw[3] || [];
    const monthCols = [];
    for (let col = 2; col < monthHeaderRow.length; col++) {
        const cell = String(monthHeaderRow[col] || '').trim().toLowerCase().slice(0, 3);
        const month = MONTH_NAMES[cell];
        if (month) monthCols.push({ col, month });
    }
    if (monthCols.length === 0) return null;

    const records = [];
    for (let i = 4; i < raw.length; i++) {
        const row = raw[i];
        if (!row) continue;
        const maker = String(row[1] || '').trim();
        if (!maker || /^total$/i.test(maker)) continue;
        for (const { col, month } of monthCols) {
            const count = parseInt(row[col]);
            if (!isNaN(count) && count > 0) records.push({ maker, month, count });
        }
    }
    return { year, records };
}

// ─── FILE PROCESSOR ───────────────────────────────────────────────────────────

async function processXlsFile(filepath, filename) {
    const meta = parseFilename(filename);
    if (!meta) return 0;

    const wb = XLSX.readFile(filepath);
    const parsed = parseSheet(wb.Sheets[wb.SheetNames[0]]);

    if (!parsed || parsed.records.length === 0) {
        await pool.query(
            'INSERT INTO processed_files (filename, row_count) VALUES ($1, 0) ON CONFLICT DO NOTHING',
            [filename]
        );
        fs.unlinkSync(filepath);
        return 0;
    }

    const { year, records } = parsed;
    const state = cache.stateByCode.get(meta.stateCode);
    const vc    = cache.vcByIdx.get(meta.vcIdx);
    const rtoId = await getRtoId(meta.stateCode, meta.rtoCode);

    if (!state || !vc || !rtoId) {
        console.warn(`  [PROCESSOR] Cannot resolve IDs for ${filename} — skipping`);
        return 0;
    }

    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        for (const { maker, month, count } of records) {
            const makerId = await getOrCreateMaker(maker);
            await client.query(
                `INSERT INTO vehicle_registrations
                     (state_id, rto_id, vehicle_class_id, maker_id, year, month, count)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (state_id, rto_id, vehicle_class_id, maker_id, year, month)
                 DO UPDATE SET count = EXCLUDED.count, created_at = NOW()`,
                [state.id, rtoId, vc.id, makerId, year, month, count]
            );
        }
        await client.query(
            `INSERT INTO processed_files (filename, row_count)
             VALUES ($1, $2)
             ON CONFLICT (filename) DO UPDATE SET row_count = $2, processed_at = NOW()`,
            [filename, records.length]
        );
        await client.query('COMMIT');
    } catch (err) {
        await client.query('ROLLBACK');
        throw err;
    } finally {
        client.release();
    }

    fs.unlinkSync(filepath);
    return records.length;
}

async function loadProcessedFiles() {
    const { rows } = await pool.query('SELECT filename FROM processed_files');
    return new Set(rows.map(r => r.filename));
}

async function closeDb() {
    await pool.end();
}

module.exports = {
    pool,
    initDb,
    loadStates,
    loadVehicleClasses,
    loadCompleted,
    saveRtos,
    markCompleted,
    logFetch,
    processXlsFile,
    loadProcessedFiles,
    closeDb,
};
