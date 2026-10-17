import * as fs from "node:fs"

/**
 * Muuntaa application-e2e.properties:n JDBC-URLin ja tunnukset `pg`-yhteysmerkkijonoksi
 * (`postgresql://user:pass@host:port/database`), jotta e2e-kannan asetukset ovat yhdessä paikassa.
 */
function getDatabaseConnectionStringFromServerE2EConfiguration(
  workerIndex: number,
) {
  const propertiesFile =
    "../server/src/main/resources/application-e2e.properties"
  const properties = readJavaPropertiesFile(propertiesFile)

  const dbUrl = parseDatabaseUrlFromJDBCUrl(
    properties["spring.datasource.url"],
    workerIndex,
  )
  const user = properties["spring.datasource.username"]
  const password = properties["spring.datasource.password"]

  return `postgresql://${user}:${password}@${dbUrl.substring("postgresql://".length)}`
}

function parseDatabaseUrlFromJDBCUrl(
  jdbcUrl: string,
  workerIndex: number,
): string {
  // The actual DB url is the part beginning with `postgresql://` up to any
  // possible JDBC parameters provided as URL query parameters. The start of
  // JDBC parameters is thus the first `?` in the string.
  const dbUrlBodyStart = jdbcUrl.indexOf("postgresql://")
  const jdbcParamsStart = jdbcUrl.indexOf("?")

  return jdbcUrl
    .replace("${TEST_PARALLEL_INDEX}", workerIndex.toString())
    .substring(
      dbUrlBodyStart,
      jdbcParamsStart < 0 ? undefined : jdbcParamsStart,
    )
}

function readJavaPropertiesFile(path: string): Record<string, string> {
  const propertiesFileContent = fs.readFileSync(path, "utf-8")
  const propertyLines = propertiesFileContent
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .filter((line) => line.includes("="))

  const propertiesEntries = propertyLines.flatMap((line) => {
    const indexOfFirstEqualsSign = line.indexOf("=")
    if (indexOfFirstEqualsSign < 0) {
      return []
    }

    const key = line.substring(0, indexOfFirstEqualsSign)
    const value = line.substring(indexOfFirstEqualsSign + 1)
    return [[key, value]]
  })
  return Object.fromEntries(propertiesEntries)
}

export type Config = ReturnType<typeof createConfig>

export const createConfig = (workerIndex: number) => {
  const port = 8080 + workerIndex
  return {
    workerIndex,
    port,
    baseUrl: `http://127.0.0.1:${port}/kielitutkinnot/`,
    database: {
      connectionString:
        getDatabaseConnectionStringFromServerE2EConfiguration(workerIndex),
    },
  } as const
}
