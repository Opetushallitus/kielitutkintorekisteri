import { defineConfig, devices } from "@playwright/test"

/**
 * Palvelin laskee "tämän päivän" aina Europe/Helsinki-vyöhykkeellä (TimeService.zoneId),
 * joten testiprosessin on oltava samassa vyöhykkeessä. Muuten CI:n UTC-ajossa testin ja
 * palvelimen päivä eroavat klo 21–24 UTC välillä ja tähän päivään sidotut odotukset kaatuvat.
 */
process.env.TZ = "Europe/Helsinki"

/**
 * Read environment variables from file.
 * https://github.com/motdotla/dotenv
 */
// import dotenv from 'dotenv';
// dotenv.config({ path: path.resolve(__dirname, '.env') });

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

/**
 * See https://playwright.dev/docs/test-configuration.
 */
export default defineConfig({
  testDir: "./tests",
  /* Run tests in files in parallel */
  fullyParallel: numWorkers > 1,
  /* Fail the build on CI if you accidentally left test.only in the source code. */
  forbidOnly: !!process.env.CI,
  /* Retry on CI only */
  retries: process.env.CI ? 2 : 0,
  /* Opt out of parallel tests on CI. */
  workers: numWorkers,
  /* Reporter to use. See https://playwright.dev/docs/test-reporters */
  reporter: "html",
  /* Shared settings for all the projects below. See https://playwright.dev/docs/api/class-testoptions. */
  use: {
    /* Base URL to use in actions like `await page.goto('/')`. */
    // baseURL: 'http://127.0.0.1:3000',

    /* Collect trace when retrying the failed test. See https://playwright.dev/docs/trace-viewer */
    trace: "on-first-retry",

    /* Sama vyöhyke kuin palvelimella, ks. process.env.TZ ylhäällä. */
    timezoneId: "Europe/Helsinki",
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },

    // {
    //   name: "firefox",
    //   use: { ...devices["Desktop Firefox"] },
    // },
    //
    // {
    //   name: "webkit",
    //   use: { ...devices["Desktop Safari"] },
    // },

    /* Test against mobile viewports. */
    // {
    //   name: 'Mobile Chrome',
    //   use: { ...devices['Pixel 5'] },
    // },
    // {
    //   name: 'Mobile Safari',
    //   use: { ...devices['iPhone 12'] },
    // },

    /* Test against branded browsers. */
    // {
    //   name: 'Microsoft Edge',
    //   use: { ...devices['Desktop Edge'], channel: 'msedge' },
    // },
    // {
    //   name: 'Google Chrome',
    //   use: { ...devices['Desktop Chrome'], channel: 'chrome' },
    // },
  ],

  /* Run your local dev server before starting the tests */
  webServer: workerWebServerConfigs,
})
