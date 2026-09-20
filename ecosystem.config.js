// pm2 config: fetch the current year once a day, then exit.
//   pm2 start ecosystem.config.js     — runs now, then daily on the schedule below
//   pm2 logs vahan-api-fetcher        — progress
//   pm2 stop vahan-api-fetcher        — skip runs (cron keeps firing until deleted)
//
// The server's clock is Europe/Berlin, and pm2 schedules in the server's timezone,
// so 00:00 IST is 20:30 there. For midnight on the server clock use '0 0 * * *'.
module.exports = {
    apps: [{
        name: 'vahan-api-fetcher',
        script: 'api_fetcher.js',
        cwd: __dirname,
        autorestart: false,          // one run per trigger; the script exits when the year is done (~70 min)
        cron_restart: '30 20 * * *', // 00:00 IST
        time: true,                  // timestamps in pm2 logs
        // YEAR is empty on purpose: it beats the YEAR line in .env (dotenv won't overwrite a set variable),
        // so the daily run always fetches the current calendar year and keeps working after 31 Dec.
        env: { CONCURRENCY: '5', YEAR: '' },
    }],
};
