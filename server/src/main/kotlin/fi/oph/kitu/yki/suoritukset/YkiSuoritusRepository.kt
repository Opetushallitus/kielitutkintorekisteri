package fi.oph.kitu.yki.suoritukset

import fi.oph.kitu.SortDirection
import fi.oph.kitu.i18n.finnishDate
import fi.oph.kitu.yki.KituArviointitila
import fi.oph.kitu.yki.TutkinnonOsa
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.buildSql
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.pagingQuery
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectArvosanat
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectSuoritukset
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectTarkistusarviointiAgg
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.selectYkiSuoritusEntity
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.whereSearchMatches
import fi.oph.kitu.yki.suoritukset.YkiSuoritusSql.withCtes
import io.opentelemetry.instrumentation.annotations.WithSpan
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate

@Service
class YkiSuoritusRepository {
    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var jdbcNamedParameterTemplate: NamedParameterJdbcTemplate

    @WithSpan
    @Transactional
    fun saveAllNewEntities(suoritukset: Iterable<YkiSuoritusEntity>): Iterable<YkiSuoritusEntity> {
        val savedSuoritukset = suoritukset.mapNotNull { save(it) }
        return findSuorituksetByIdList(savedSuoritukset)
    }

    private fun findSuorituksetByIdList(ids: List<Int>): Iterable<YkiSuoritusEntity> {
        if (ids.isEmpty()) return emptyList()
        val suoritusIds = ids.joinToString(",", "(", ")")
        return jdbcTemplate.query(
            selectSuoritukset(viimeisin = true, "WHERE yki_suoritus.id IN $suoritusIds"),
            YkiSuoritusEntity.fromRow,
        )
    }

    fun find(
        searchBy: String = "",
        column: YkiSuoritusColumn = YkiSuoritusColumn.Tutkintopaiva,
        direction: SortDirection = SortDirection.DESC,
        distinct: Boolean = true,
        limit: Int? = null,
        offset: Int? = null,
    ): Iterable<YkiSuoritusEntity> =
        jdbcNamedParameterTemplate.query(
            selectSuoritukset(
                distinct,
                whereSearchMatches("search_str"),
                "ORDER BY ${column.entityName} $direction",
                pagingQuery(limit, offset),
            ),
            mapOf(
                "search_str" to "%$searchBy%",
                "limit" to limit,
                "offset" to offset,
            ),
            YkiSuoritusEntity.fromRow,
        )

    fun findSuorituksetWithNoKoskiopiskeluoikeus(): Iterable<YkiSuoritusEntity> =
        jdbcNamedParameterTemplate.query(
            selectSuoritukset(viimeisin = true, "WHERE NOT koski_siirto_kasitelty"),
            YkiSuoritusEntity.fromRow,
        )

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
            buildSql(
                withCtes("viimeisin_suoritus" to selectSuoritukset(viimeisin = distinct, whereSearchMatches())),
                "SELECT COUNT(*) FROM viimeisin_suoritus",
            )

        val params = mapOf("search_str" to "%$searchBy%")

        return jdbcNamedParameterTemplate.queryForObject(
            sql,
            params,
            Long::class.java,
        )
            ?: 0
    }

    fun findLatestBySuoritusIds(ids: List<Int>): List<YkiSuoritusEntity> =
        if (ids.isEmpty()) {
            emptyList()
        } else {
            jdbcNamedParameterTemplate.query(
                selectSuoritukset(viimeisin = true, "WHERE yki_suoritus.suoritus_id IN (:ids)"),
                mapOf("ids" to ids),
                YkiSuoritusEntity.fromRow,
            )
        }

    @Transactional
    fun save(suoritus: YkiSuoritusEntity): Int? =
        insertSuoritus(suoritus)?.let { suoritusId ->

            val osakokeet = suoritus.osakokeet()
            val osakoeIds =
                osakokeet.associate {
                    it.tyyppi to
                        insertOsakoe(
                            suoritusId,
                            it.tyyppi,
                            it.arviointipaiva,
                            it.arvosana,
                        )
                }

            if (suoritus.tarkistusarvioinninAsiatunnus != null && suoritus.tarkistusarvioinninSaapumisPvm != null) {
                suoritus.tarkistusarvioidutOsakokeet?.let {
                    val tarkistusarviointiId = insertTarkistusarviointi(suoritus)
                    suoritus.tarkistusarvioidutOsakokeet.forEach { osakoe ->
                        osakoeIds[osakoe]?.let { osakoeId ->
                            insertOsakoeTarkistusarviointiJoin(
                                osakoeId,
                                tarkistusarviointiId,
                                suoritus.arvosanaMuuttui?.contains(osakoe),
                            )
                        }
                    }
                }
            }

            suoritusId
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
        jdbcTemplate.execute("TRUNCATE TABLE yki_suoritus CASCADE")
    }

    private fun insertSuoritus(suoritus: YkiSuoritusEntity): Int? {
        val values =
            mapOf(
                "suorittajan_oid" to suoritus.suorittajanOID.toString(),
                "sukunimi" to suoritus.sukunimi,
                "etunimet" to suoritus.etunimet,
                "tutkintopaiva" to suoritus.tutkintopaiva,
                "tutkintokieli" to suoritus.tutkintokieli.toString(),
                "tutkintotaso" to suoritus.tutkintotaso.toString(),
                "jarjestajan_tunnus_oid" to suoritus.jarjestajanTunnusOid.toString(),
                "jarjestajan_nimi" to suoritus.jarjestajanNimi,
                "hetu" to suoritus.hetu,
                "sukupuoli" to suoritus.sukupuoli.toString(),
                "kansalaisuus" to suoritus.kansalaisuus,
                "katuosoite" to suoritus.katuosoite,
                "postinumero" to suoritus.postinumero,
                "postitoimipaikka" to suoritus.postitoimipaikka,
                "email" to suoritus.email,
                "suoritus_id" to suoritus.suoritusId.toString(),
                "last_modified" to Timestamp(suoritus.lastModified.toEpochMilli()),
                "koski_opiskeluoikeus" to suoritus.koskiOpiskeluoikeus?.toString(),
                "koski_siirto_kasitelty" to (suoritus.koskiSiirtoKasitelty ?: false),
                "arviointitila" to suoritus.arviointitila.toString(),
            )
        return insertInto(
            table = "yki_suoritus",
            values = values,
            onConflict = UpdateOnConflict(Constraint("unique_suoritus"), values.keys),
        )
    }

    private fun insertOsakoe(
        suoritusId: Int,
        tyyppi: TutkinnonOsa,
        arviointipaiva: LocalDate?,
        arvosana: Int?,
    ): Int =
        insertInto(
            "yki_osakoe",
            mapOf(
                "suoritus_id" to suoritusId,
                "tyyppi" to tyyppi.toString(),
                "arviointipaiva" to arviointipaiva,
                "arvosana" to arvosana,
            ),
            onConflict =
                UpdateOnConflict(
                    Columns.of("suoritus_id", "tyyppi"),
                    listOf("arviointipaiva", "arvosana"),
                ),
        )!!

    private fun insertTarkistusarviointi(suoritus: YkiSuoritusEntity): Int =
        insertInto(
            "yki_tarkistusarviointi",
            mapOf(
                "saapumispaiva" to suoritus.tarkistusarvioinninSaapumisPvm,
                "kasittelypaiva" to suoritus.tarkistusarvioinninKasittelyPvm,
                "asiatunnus" to suoritus.tarkistusarvioinninAsiatunnus,
                "perustelu" to suoritus.perustelu,
            ),
        )!!

    private fun insertOsakoeTarkistusarviointiJoin(
        osakoeId: Int,
        tarkistusarvointiId: Int,
        arvosanaMuuttui: Boolean?,
    ) = insertInto<Unit>(
        "yki_osakoe_tarkistusarviointi",
        mapOf(
            "osakoe_id" to osakoeId,
            "tarkistusarviointi_id" to tarkistusarvointiId,
            "arvosana_muuttui" to arvosanaMuuttui,
        ),
        returning = null,
    )

    private inline fun <reified T> insertInto(
        table: String,
        values: Map<String, Any?>,
        onConflict: ConflictHandler? = null,
        returning: String? = "id",
    ): T? {
        require(values.isNotEmpty()) { "values must not be empty" }
        if (onConflict is OnConflictDoNothing) {
            require(returning == null) { "cannot use OnConflictDoNothing while returning $returning" }
        }

        val sql =
            """
            INSERT INTO $table (${values.keys.joinToString(",\n")})
            VALUES (${values.values.joinToString(",") { "?" }})
            ${onConflict?.toString().orEmpty()}
            ${returning?.let { "RETURNING $it" }.orEmpty()}
            """.trimIndent()

        return if (returning == null) {
            jdbcTemplate.update(sql, *values.values.toTypedArray())
            null
        } else {
            jdbcTemplate
                .queryForList(sql, T::class.java, *values.values.toTypedArray())
                .firstOrNull()
        }
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
                yki_suoritus_lisatieto.tarkistusarviointi_hyvaksytty_pvm as tarkistusarviointi_hyvaksytty_pvm,
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

interface ConflictHandler {
    val conflictTarget: ConflictTarget
}

data class OnConflictDoNothing(
    override val conflictTarget: ConflictTarget,
) : ConflictHandler {
    override fun toString() = "ON CONFLICT ${conflictTarget }DO NOTHING"
}

data class UpdateOnConflict(
    override val conflictTarget: ConflictTarget,
    val columns: Iterable<String>,
) : ConflictHandler {
    override fun toString() =
        """
        ON CONFLICT $conflictTarget 
        DO UPDATE SET 
        ${columns.joinToString(",\n") { "$it = EXCLUDED.$it" }}
        """.trimIndent()
}

sealed interface ConflictTarget

data class Constraint(
    val name: String,
) : ConflictTarget {
    override fun toString() = "ON CONSTRAINT $name"
}

data class Columns(
    val names: Iterable<String>,
) : ConflictTarget {
    override fun toString() = names.joinToString(", ", "(", ")")

    companion object {
        fun of(vararg names: String) = Columns(names.toList())
    }
}
