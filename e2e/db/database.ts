import * as pg from "pg"
import * as assert from "node:assert"
import SQL, { SQLStatement } from "sql-template-strings"
import { Config } from "../config"

const createQuery =
  (client: pg.PoolClient) =>
  async <T extends {}>(statement: string | SQLStatement) => {
    const { rows } = await client.query(statement)
    return rows as T[]
  }

const createQueryOne =
  (client: pg.PoolClient) =>
  async <T extends {}>(statement: string | SQLStatement) => {
    const { rows } = await client.query(statement)

    assert(
      rows.length === 1,
      `'queryOne' should always return one row, got ${rows.length}`,
    )

    return rows[0] as T
  }

const createQueryOneOrNone =
  (client: pg.PoolClient) =>
  async <T extends {}>(statement: string | SQLStatement): Promise<T | null> => {
    const { rows } = await client.query(statement)

    assert(
      rows.length === 1 || rows.length === 0,
      `'queryOneOrNone' should always return one or zero rows, got ${rows.length}`,
    )

    return (rows[0] as T | null | undefined) ?? null
  }

const query: (pool: pg.Pool) => ReturnType<typeof createQuery> =
  (pool: pg.Pool) =>
  async (...args) => {
    const client = await pool.connect()
    try {
      return createQuery(client)(...args)
    } finally {
      client.release()
    }
  }

const queryOne: (pool: pg.Pool) => ReturnType<typeof createQueryOne> =
  (pool: pg.Pool) =>
  async (...args) => {
    const client = await pool.connect()
    try {
      return createQueryOne(client)(...args)
    } finally {
      client.release()
    }
  }

const queryOneOrNone: (
  pool: pg.Pool,
) => ReturnType<typeof createQueryOneOrNone> =
  (pool: pg.Pool) =>
  async (...args) => {
    const client = await pool.connect()
    try {
      return createQueryOneOrNone(client)(...args)
    } finally {
      client.release()
    }
  }

export interface TransactionClient {
  query: ReturnType<typeof query>
  queryOne: ReturnType<typeof queryOne>
  queryOneOrNone: ReturnType<typeof queryOneOrNone>
}

const transact =
  (pool: pg.Pool) =>
  async <T>(transaction: (client: TransactionClient) => Promise<T>) => {
    const client = await pool.connect()
    try {
      await client.query("BEGIN")
      const res = await transaction({
        query: createQuery(client),
        queryOne: createQueryOne(client),
        queryOneOrNone: createQueryOneOrNone(client),
      })
      await client.query("COMMIT")
      return res
    } catch (err) {
      await client.query("ROLLBACK")
      throw err
    } finally {
      client.release()
    }
  }

const withEmptyDatabase = (pool: pg.Pool) => async () => {
  const client = await pool.connect()
  try {
    await client.query(SQL`
      TRUNCATE TABLE
        koto_suoritus,
        koto_suoritus_error,
        yki_suoritus,
        yki_suoritus_lisatieto,
        yki_arvioija,
        yki_arviointioikeus,
        yki_arvioija_arviointikausi,
        yki_arvioija_arviointikausi_oikeus,
        vkt_suoritus,
        koski_error
      RESTART IDENTITY CASCADE
    `)
  } finally {
    client.release()
  }
}

export const createTestDatabase = (config: Config) => {
  const pool = new pg.Pool({
    connectionString: config.database.connectionString,
    ssl: false,
  })

  return {
    dbClient: {
      pool,
      query: query(pool),
      queryOne: queryOne(pool),
      queryOneOrNone: queryOneOrNone(pool),
      transact: transact(pool),
    },
    withEmptyDatabase: withEmptyDatabase(pool),
  }
}
