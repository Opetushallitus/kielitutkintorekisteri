package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.SortDirection
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.yki.KituArviointitila
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.buildSql
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.pagingQuery
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectArvosanat
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectQuery
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectSuoritukset
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectTarkistusarviointiAgg
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectYkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.whereSearchMatches
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.withCtes
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.BatchPreparedStatementSetter
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementCreatorFactory
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.support.GeneratedKeyHolder
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.sql.PreparedStatement
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

@Service
class YkiSuoritusRepository {
    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var jdbcNamedParameterTemplate: NamedParameterJdbcTemplate

    private val allColumns =
        """
        id,
        suorittajan_oid,
        hetu,
        sukupuoli,
        sukunimi,
        etunimet,
        kansalaisuus,
        katuosoite,
        postinumero,
        postitoimipaikka,
        email,
        yki_suoritus.suoritus_id,
        last_modified,
        tutkintopaiva,
        tutkintokieli,
        tutkintotaso,
        jarjestajan_tunnus_oid,
        jarjestajan_nimi,
        arviointipaiva,
        tekstin_ymmartaminen,
        kirjoittaminen,
        rakenteet_ja_sanasto,
        puheen_ymmartaminen,
        puhuminen,
        yleisarvosana,
        tarkistusarvioinnin_saapumis_pvm,
        tarkistusarvioinnin_asiatunnus,
        tarkistusarvioidut_osakokeet,
        arvosana_muuttui,
        perustelu,
        tarkistusarvioinnin_kasittely_pvm,
        tarkistusarviointi_hyvaksytty_pvm,
        koski_opiskeluoikeus,
        koski_siirto_kasitelty,
        arviointitila
        """.trimIndent()

    /**
     * Override to allow handling duplicates/conflicts. The default implementation from CrudRepository fails
     * due to the unique constraint. Overriding the implementation allows explicit handling of conflicts.
     */
    @WithSpan
    fun saveAllNewEntities(suoritukset: Iterable<YkiSuoritusEntity>): Iterable<YkiSuoritusEntity> {
        val sql =
            """
            INSERT INTO yki_suoritus (
                suorittajan_oid,
                hetu,
                sukupuoli,
                sukunimi,
                etunimet,
                kansalaisuus,
                katuosoite,
                postinumero,
                postitoimipaikka,
                email,
                suoritus_id,
                last_modified,
                tutkintopaiva,
                tutkintokieli,
                tutkintotaso,
                jarjestajan_tunnus_oid,
                jarjestajan_nimi,
                arviointipaiva,
                tekstin_ymmartaminen,
                kirjoittaminen,
                rakenteet_ja_sanasto,
                puheen_ymmartaminen,
                puhuminen,
                yleisarvosana,
                tarkistusarvioinnin_saapumis_pvm,
                tarkistusarvioinnin_asiatunnus,
                tarkistusarvioidut_osakokeet,
                arvosana_muuttui,
                perustelu,
                tarkistusarvioinnin_kasittely_pvm,
                koski_opiskeluoikeus,
                koski_siirto_kasitelty,
                arviointitila
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT ON CONSTRAINT unique_suoritus DO NOTHING;
            """.trimIndent()
        val pscf = PreparedStatementCreatorFactory(sql)
        pscf.setGeneratedKeysColumnNames("id")
        val preparedStatementCreator = pscf.newPreparedStatementCreator(sql, null)

        val batchPreparedStatementSetter =
            object : BatchPreparedStatementSetter {
                override fun setValues(
                    ps: PreparedStatement,
                    i: Int,
                ) {
                    val suoritus = suoritukset.elementAt(i)
                    setInsertValues(ps, suoritus)
                }

                override fun getBatchSize() = suoritukset.count()
            }

        val keyHolder = GeneratedKeyHolder()

        jdbcTemplate.batchUpdate(
            preparedStatementCreator,
            batchPreparedStatementSetter,
            keyHolder,
        )

        val savedSuoritukset = keyHolder.keyList.map { it["id"] as Int }

        return if (savedSuoritukset.isEmpty()) listOf() else findSuorituksetByIdList(savedSuoritukset)
    }

    private fun findSuorituksetByIdList(ids: List<Int>): Iterable<YkiSuoritusEntity> {
        val suoritusIds = ids.joinToString(",", "(", ")")
        val findSavedQuerySql =
            """
            SELECT $allColumns
            $fromYkiSuoritus
            WHERE id IN $suoritusIds
            """.trimIndent()
        return jdbcTemplate.query(findSavedQuerySql, YkiSuoritusEntity.fromRow)
    }

    private val fromYkiSuoritus =
        """
        FROM yki_suoritus 
        LEFT JOIN yki_suoritus_lisatieto ON yki_suoritus.suoritus_id = yki_suoritus_lisatieto.suoritus_id
        """.trimIndent()

    fun find(
        searchBy: String = "",
        column: YkiSuoritusColumn = YkiSuoritusColumn.Tutkintopaiva,
        direction: SortDirection = SortDirection.DESC,
        distinct: Boolean = true,
        limit: Int? = null,
        offset: Int? = null,
    ): Iterable<YkiSuoritusEntity> {
        val searchStr = "%$searchBy%"
        val findAllQuerySql =
            selectSuoritukset(
                distinct,
                whereSearchMatches("search_str"),
                "ORDER BY ${column.entityName} $direction",
                pagingQuery(limit, offset),
            )

        val params =
            mapOf(
                "search_str" to searchStr,
                "limit" to limit,
                "offset" to offset,
            )

        return jdbcNamedParameterTemplate.query(findAllQuerySql, params, YkiSuoritusEntity.fromRow)
    }

    fun findSuorituksetWithNoKoskiopiskeluoikeus(): Iterable<YkiSuoritusEntity> {
        val sql = selectSuoritukset(viimeisin = true, "WHERE NOT koski_siirto_kasitelty")
        return jdbcNamedParameterTemplate.query(sql, YkiSuoritusEntity.fromRow)
    }

    fun findTarkistusarvoidutSuoritukset(): Iterable<YkiSuoritusEntity> =
        jdbcTemplate
            .query(
                selectSuoritukset(viimeisin = true, "WHERE arviointitila = ? OR arviointitila = ?"),
                YkiSuoritusEntity.fromRow,
                KituArviointitila.TARKISTUSARVIOITU.name,
                KituArviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY.name,
            ).sortedWith(
                compareByDescending(YkiSuoritusEntity::tarkistusarvioinninKasittelyPvm)
                    .thenByDescending { it.tarkistusarvioinninSaapumisPvm },
            )

    @Transactional
    fun hyvaksyTarkistusarvioinnit(
        suoritusIds: List<Int>,
        pvm: LocalDate,
    ): Int {
        findLatestBySuoritusIds(suoritusIds).forEach { suoritus ->
            val suorituksenNimi by lazy {
                "'${suoritus.suorittajanOID} ${suoritus.sukunimi} ${suoritus.etunimet}, ${suoritus.tutkintotaso} ${suoritus.tutkintokieli}'"
            }
            if (!suoritus.arviointitila.tarkistusarvioitu()) {
                throw IllegalStateException(
                    "Tarkistusarvioimatonta suoritusta $suorituksenNimi ei voi asettaa hyväksytyksi",
                )
            }
            if (suoritus.tarkistusarvioinninKasittelyPvm == null) {
                throw IllegalStateException(
                    "Tarkistusarviointia suoritukselle $suorituksenNimi ei voi hyväksyä, ennen kuin se on käsitelty.",
                )
            }
            if (suoritus.tarkistusarvioinninKasittelyPvm.isAfter(pvm)) {
                throw IllegalStateException(
                    "Tarkistusarviointi suoritukselle $suorituksenNimi ei voi hyväksyä päivämäärällä ${pvm.finnishDate()}, koska se on aiemmin kuin käsittelypäivä ${suoritus.tarkistusarvioinninKasittelyPvm.finnishDate()}.",
                )
            }
            save(
                suoritus.copy(
                    id = null,
                    arviointitila = KituArviointitila.TARKISTUSARVIOINTI_HYVAKSYTTY,
                    lastModified = Instant.now(),
                ),
            )
        }

        return jdbcTemplate.update(
            """
            INSERT INTO yki_suoritus_lisatieto (suoritus_id, tarkistusarviointi_hyvaksytty_pvm)
                VALUES ${suoritusIds.joinToString(",") { "(?, ?)" }}
            ON CONFLICT ON CONSTRAINT yki_suoritus_lisatieto_pkey
                DO UPDATE SET
                    tarkistusarviointi_hyvaksytty_pvm = EXCLUDED.tarkistusarviointi_hyvaksytty_pvm
            """.trimIndent(),
            *suoritusIds.flatMap { listOf(it, pvm) }.toTypedArray<Any>(),
        )
    }

    fun countSuoritukset(
        searchBy: String = "",
        distinct: Boolean = true,
    ): Long {
        val sql =
            if (searchBy.isEmpty()) {
                ""
            } else {
                buildSql(
                    withCtes("viimeisin_suoritus" to selectSuoritukset(viimeisin = distinct, whereSearchMatches())),
                    """
            SELECT COUNT(*) FROM viimeisin_suoritus
            """,
                )
            }
        val searchStr = "%$searchBy%"
        val params =
            mapOf(
                "search_str" to searchStr,
            )
        return jdbcNamedParameterTemplate.queryForObject(
            sql,
            params,
            Long::class.java,
        )
            ?: 0
    }

    fun findLatestBySuoritusIdsUnsafe(ids: List<Int>): List<YkiSuoritusEntity> =
        jdbcNamedParameterTemplate.query(
            selectSuoritukset(viimeisin = true, "WHERE yki_suoritus.suoritus_id IN (:ids)"),
            mapOf("ids" to ids),
            YkiSuoritusEntity.fromRow,
        )

    fun findLatestBySuoritusIds(ids: List<Int>): List<YkiSuoritusEntity> =
        if (ids.isEmpty()) emptyList() else findLatestBySuoritusIdsUnsafe(ids)

    fun save(suoritus: YkiSuoritusEntity) {
        val query =
            """
            INSERT INTO yki_suoritus (
                suorittajan_oid,
                hetu,
                sukupuoli,
                sukunimi,
                etunimet,
                kansalaisuus,
                katuosoite,
                postinumero,
                postitoimipaikka,
                email,
                suoritus_id,
                last_modified,
                tutkintopaiva,
                tutkintokieli,
                tutkintotaso,
                jarjestajan_tunnus_oid,
                jarjestajan_nimi,
                arviointipaiva,
                tekstin_ymmartaminen,
                kirjoittaminen,
                rakenteet_ja_sanasto,
                puheen_ymmartaminen,
                puhuminen,
                yleisarvosana,
                tarkistusarvioinnin_saapumis_pvm,
                tarkistusarvioinnin_asiatunnus,
                tarkistusarvioidut_osakokeet,
                arvosana_muuttui,
                perustelu,
                tarkistusarvioinnin_kasittely_pvm,
                koski_opiskeluoikeus,
                koski_siirto_kasitelty,
                arviointitila
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT ON CONSTRAINT unique_suoritus DO UPDATE SET
                suorittajan_oid = EXCLUDED.suorittajan_oid,
                hetu = EXCLUDED.hetu,
                sukupuoli = EXCLUDED.sukupuoli,
                sukunimi = EXCLUDED.sukunimi,
                etunimet = EXCLUDED.etunimet,
                kansalaisuus = EXCLUDED.kansalaisuus,
                katuosoite = EXCLUDED.katuosoite,
                postinumero = EXCLUDED.postinumero,
                postitoimipaikka = EXCLUDED.postitoimipaikka,
                email = EXCLUDED.email,
                tutkintopaiva = EXCLUDED.tutkintopaiva,
                tutkintokieli = EXCLUDED.tutkintokieli,
                tutkintotaso = EXCLUDED.tutkintotaso,
                jarjestajan_tunnus_oid = EXCLUDED.jarjestajan_tunnus_oid,
                jarjestajan_nimi = EXCLUDED.jarjestajan_nimi,
                arviointipaiva = EXCLUDED.arviointipaiva,
                tekstin_ymmartaminen = EXCLUDED.tekstin_ymmartaminen,
                kirjoittaminen = EXCLUDED.kirjoittaminen,
                rakenteet_ja_sanasto = EXCLUDED.rakenteet_ja_sanasto,
                puheen_ymmartaminen = EXCLUDED.puheen_ymmartaminen,
                puhuminen = EXCLUDED.puhuminen,
                yleisarvosana = EXCLUDED.yleisarvosana,
                tarkistusarvioinnin_saapumis_pvm = EXCLUDED.tarkistusarvioinnin_saapumis_pvm,
                tarkistusarvioinnin_asiatunnus = EXCLUDED.tarkistusarvioinnin_asiatunnus,
                tarkistusarvioidut_osakokeet = EXCLUDED.tarkistusarvioidut_osakokeet,
                arvosana_muuttui = EXCLUDED.arvosana_muuttui,
                perustelu = EXCLUDED.perustelu,
                tarkistusarvioinnin_kasittely_pvm = EXCLUDED.tarkistusarvioinnin_kasittely_pvm,
                koski_opiskeluoikeus = EXCLUDED.koski_opiskeluoikeus,
                koski_siirto_kasitelty = EXCLUDED.koski_siirto_kasitelty,
                arviointitila = EXCLUDED.arviointitila
            """.trimIndent()

        jdbcTemplate.update(query) {
            setInsertValues(it, suoritus)
        }
    }

    fun findAll(): List<YkiSuoritusEntity> =
        jdbcTemplate.query(
            buildSql(
                withCtes(
                    "arvosana" to selectArvosanat(),
                    "tarkistusarviointi_agg" to selectTarkistusarviointiAgg(),
                ),
                selectYkiSuoritusEntity(
                    ykiSuoritusTable = "yki_suoritus",
                    arvosanaTable = "arvosana",
                    tarkistusarvointiAggregationTable = "tarkistusarviointi_agg",
                ),
            ),
            YkiSuoritusEntity.fromRow,
        )

    fun findSuorituksetWithUnsentArvioinninTila(): List<YkiSuoritusEntity> =
        jdbcTemplate
            .query(
                buildSql(
                    selectSuoritukset(viimeisin = true),
                    """
                        WHERE
                            arviointitila_lahetetty IS NULL
                            OR arviointitila_lahetetty < last_modified
                        ORDER BY
                            yki_suoritus.suoritus_id,
                            last_modified DESC 
                    """,
                ),
                YkiSuoritusEntity.fromRow,
            )

    fun setArvioinninTilaSent(suoritusId: Int) = setArvioinninTilaSent(listOf(suoritusId))

    fun setArvioinninTilaSent(suoritusIds: List<Int>) =
        if (suoritusIds.isNotEmpty()) {
            jdbcTemplate.update(
                """
                INSERT INTO yki_suoritus_lisatieto (suoritus_id, arviointitila_lahetetty)
                    VALUES ${suoritusIds.joinToString(",") { "(?, now())" }}
                ON CONFLICT ON CONSTRAINT yki_suoritus_lisatieto_pkey
                    DO UPDATE SET
                        arviointitila_lahetetty = now();
                """.trimIndent(),
                *suoritusIds.toTypedArray(),
            )
        } else {
            0
        }

    fun deleteAll() {
        jdbcTemplate.execute("TRUNCATE TABLE yki_suoritus_lisatieto")
        jdbcTemplate.execute("TRUNCATE TABLE yki_suoritus")
    }

    private fun setInsertValues(
        ps: PreparedStatement,
        suoritus: YkiSuoritusEntity,
    ) {
        ps.setString(1, suoritus.suorittajanOID.toString())
        ps.setString(2, suoritus.hetu)
        ps.setString(3, suoritus.sukupuoli.toString())
        ps.setString(4, suoritus.sukunimi)
        ps.setString(5, suoritus.etunimet)
        ps.setString(6, suoritus.kansalaisuus)
        ps.setString(7, suoritus.katuosoite)
        ps.setString(8, suoritus.postinumero)
        ps.setString(9, suoritus.postitoimipaikka)
        ps.setString(10, suoritus.email)
        ps.setInt(11, suoritus.suoritusId)
        ps.setTimestamp(12, Timestamp(suoritus.lastModified.toEpochMilli()))
        ps.setObject(13, suoritus.tutkintopaiva)
        ps.setString(14, suoritus.tutkintokieli.toString())
        ps.setString(15, suoritus.tutkintotaso.toString())
        ps.setString(16, suoritus.jarjestajanTunnusOid.toString())
        ps.setString(17, suoritus.jarjestajanNimi)
        ps.setObject(18, suoritus.arviointipaiva)
        ps.setObject(19, suoritus.tekstinYmmartaminen)
        ps.setObject(20, suoritus.kirjoittaminen)
        ps.setObject(21, suoritus.rakenteetJaSanasto)
        ps.setObject(22, suoritus.puheenYmmartaminen)
        ps.setObject(23, suoritus.puhuminen)
        ps.setObject(24, suoritus.yleisarvosana)
        ps.setObject(25, suoritus.tarkistusarvioinninSaapumisPvm)
        ps.setObject(26, suoritus.tarkistusarvioinninAsiatunnus)
        ps.setArray(
            27,
            ps.connection.createArrayOf(
                "text",
                suoritus.tarkistusarvioidutOsakokeet?.toTypedArray(),
            ),
        )
        ps.setArray(
            28,
            ps.connection.createArrayOf(
                "text",
                suoritus.arvosanaMuuttui?.toTypedArray(),
            ),
        )
        ps.setObject(29, suoritus.perustelu)
        ps.setObject(30, suoritus.tarkistusarvioinninKasittelyPvm)
        ps.setString(31, suoritus.koskiOpiskeluoikeus?.toString())
        ps.setBoolean(32, suoritus.koskiSiirtoKasitelty ?: false)
        ps.setString(33, suoritus.arviointitila.toString())
    }
}

object YkiSuoritusSql {
    fun buildSql(vararg parts: String?) = parts.filterNotNull().joinToString("\n").trimIndent()

    fun selectSuoritukset(
        viimeisin: Boolean,
        vararg conditions: String?,
    ) = buildSql(
        withCtes(
            "suoritus" to selectRootSuoritukset(viimeisin),
            "arvosana" to selectArvosanat(),
            "tarkistusarviointi_agg" to selectTarkistusarviointiAgg(),
        ),
        selectYkiSuoritusEntity(
            ykiSuoritusTable = "suoritus",
            arvosanaTable = "arvosana",
            tarkistusarvointiAggregationTable = "tarkistusarviointi_agg",
        ),
        *conditions,
    )

    fun selectRootSuoritukset(viimeisin: Boolean = true) =
        """
        ${selectQuery(viimeisin)}
        FROM yki_suoritus
        ORDER BY
            suoritus_id,
            last_modified DESC
        """.trimIndent()

    fun selectArvosanat(ykiSuoritusTable: String = "yki_suoritus") =
        """
        SELECT
            yki_suoritus.id as suoritus_id,
            max(arviointipaiva) AS arviointipaiva,
            max(arvosana) FILTER (WHERE tyyppi = 'PU') AS puhuminen,
            max(arvosana) FILTER (WHERE tyyppi = 'KI') AS kirjoittaminen,
            max(arvosana) FILTER (WHERE tyyppi = 'TY') AS tekstin_ymmartaminen,
            max(arvosana) FILTER (WHERE tyyppi = 'PY') AS puheen_ymmartaminen,
            max(arvosana) FILTER (WHERE tyyppi = 'RS') AS rakenteet_ja_sanasto,
            max(arvosana) FILTER (WHERE tyyppi = 'YL') AS yleisarvosana
        FROM
            $ykiSuoritusTable AS yki_suoritus
            JOIN yki_osakoe ON yki_suoritus.id = yki_osakoe.suoritus_id
        GROUP BY
            yki_suoritus.id
        """.trimIndent()

    fun selectTarkistusarviointiAgg(ykiSuoritusTable: String = "yki_suoritus") =
        """
        SELECT
            yki_suoritus.id AS suoritus_id,
            yki_osakoe_tarkistusarviointi.tarkistusarviointi_id,
            array_agg(yki_osakoe.tyyppi) AS tarkistusarvioidut_osakokeet,
            array_agg(yki_osakoe.tyyppi) FILTER (WHERE arvosana_muuttui) AS arvosana_muuttui
        FROM
            $ykiSuoritusTable AS yki_suoritus
            LEFT JOIN yki_osakoe ON yki_osakoe.suoritus_id = yki_suoritus.id
            LEFT JOIN yki_osakoe_tarkistusarviointi ON yki_osakoe.id = yki_osakoe_tarkistusarviointi.osakoe_id
        WHERE
            tarkistusarviointi_id IS NOT NULL
        GROUP BY
            yki_suoritus.id,
            yki_osakoe_tarkistusarviointi.tarkistusarviointi_id
        """.trimIndent()

    fun selectYkiSuoritusEntity(
        ykiSuoritusTable: String,
        arvosanaTable: String,
        tarkistusarvointiAggregationTable: String,
    ) = """
        SELECT
                yki_suoritus.*,
                arvosana.*,
                yki_suoritus_lisatieto.arviointitila_lahetetty,
                tarkistusarviointi_agg.tarkistusarvioidut_osakokeet,
                tarkistusarviointi_agg.arvosana_muuttui,
                yki_tarkistusarviointi.asiatunnus as tarkistusarvioinnin_asiatunnus,
                yki_tarkistusarviointi.saapumispaiva as tarkistusarvioinnin_saapumis_pvm,
                yki_tarkistusarviointi.kasittelypaiva as tarkistusarvioinnin_kasittely_pvm,
                yki_tarkistusarviointi.hyvaksymispaiva as tarkistusarviointi_hyvaksytty_pvm,
                yki_tarkistusarviointi.perustelu
            FROM
                $ykiSuoritusTable AS yki_suoritus
                LEFT JOIN $arvosanaTable AS arvosana ON arvosana.suoritus_id = yki_suoritus.id
                LEFT JOIN $tarkistusarvointiAggregationTable AS tarkistusarviointi_agg ON tarkistusarviointi_agg.suoritus_id = yki_suoritus.id
                LEFT JOIN yki_tarkistusarviointi ON yki_tarkistusarviointi.id = tarkistusarviointi_agg.tarkistusarviointi_id
                LEFT JOIN yki_suoritus_lisatieto ON yki_suoritus.suoritus_id = yki_suoritus_lisatieto.suoritus_id
        """.trimIndent()

    fun selectQuery(
        distinct: Boolean,
        columns: String = "*",
    ): String = if (distinct) "SELECT DISTINCT ON (yki_suoritus.suoritus_id) $columns" else "SELECT $columns"

    fun pagingQuery(
        limit: Int?,
        offset: Int?,
    ): String = if (limit != null && offset != null) "LIMIT :limit OFFSET :offset" else ""

    fun whereSearchMatches(paramName: String = "search_str"): String =
        """
        WHERE suorittajan_oid ILIKE :$paramName 
            OR etunimet ILIKE :$paramName
            OR sukunimi ILIKE :$paramName
            OR email ILIKE :$paramName
            OR hetu ILIKE :$paramName
            OR jarjestajan_tunnus_oid ILIKE :$paramName 
            OR jarjestajan_nimi ILIKE :$paramName
        """.trimIndent()

    fun withCtes(vararg ctes: Pair<String, String>) =
        """
        WITH ${ctes.joinToString(",\n") { "${it.first} AS (${it.second})" }}
        """.trimIndent()
}
