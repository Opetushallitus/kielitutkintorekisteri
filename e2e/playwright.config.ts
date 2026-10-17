import { defineConfig, devices } from "@playwright/test"

/**
 * Palvelin laskee "tämän päivän" aina Europe/Helsinki-vyöhykkeellä (TimeService.zoneId),
 * joten testiprosessin on oltava samassa vyöhykkeessä. Muuten CI:n UTC-ajossa testin ja
 * palvelimen päivä eroavat klo 21–24 UTC välillä ja tähän päivään sidotut odotukset kaatuvat.
 */
process.env.TZ = "Europe/Helsinki"

const numWorkers = process.env.CI
  ? 1
  : parseInt(process.env.TEST_WORKERS ?? "1")

const workerWebServer = (workerIndex: number) => {
  const port = 8080 + workerIndex
  return {
    timeout: 5 * 60000,
    command: "./mvnw spring-boot:run",
    url: `http://localhost:${port}/kielitutkinnot`,
    reuseExistingServer: false,
    cwd: "../server/",
    env: {
      SPRING_PROFILES_ACTIVE: "e2e",
      // per-worker settings (e.g. assign each worker unique DB and port)
      TEST_WORKER_PORT: port.toString(),
      TEST_PARALLEL_INDEX: workerIndex.toString(),
    },
    /* Pipe server logs to console for troubleshooting */
    stdout: "pipe",
    stderr: "pipe",
  } as const
}

const workerWebServerConfigs = Array(numWorkers)
  .fill(0)
  .map((_, workerIndex) => workerWebServer(workerIndex))

export default defineConfig({
  testDir: "./tests",
  fullyParallel: numWorkers > 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: numWorkers,
  reporter: "html",
  use: {
    trace: "on-first-retry",

    /* Sama vyöhyke kuin palvelimella, ks. process.env.TZ ylhäällä. */
    timezoneId: "Europe/Helsinki",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  /* Jokainen worker käynnistää oman palvelimensa omaan porttiinsa ja kantaansa. */
  webServer: workerWebServerConfigs,
})
