const cron = require("node-cron");

console.log("Application started...");

cron.schedule("*/5 * * * * *", () => {
  console.log("Running every 5 seconds:", new Date().toLocaleString());
});

// Keep the process alive
process.stdin.resume();